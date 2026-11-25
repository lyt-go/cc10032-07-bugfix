"""merge_plan 领域服务：流批合并策略与发布完整性门禁。

合并入口只接收已通过 batch_slice 完整性核对的同表、同键列、同区间输入；
中断批次只能在结果中反映剩余工作，不能产生第二份“成功”的发布制品。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MergePlanService(BaseModuleService):
    MODULE = "merge_plan"
    ENTITY = "合并计划"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["计划编号", "流式来源", "批式来源", "合并键列", "冲突解决", "输出路径", "合并窗口", "去重策略"]
    REQUIRED_FIELDS = ["计划编号", "流式来源", "批式来源"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已完成"]
    ACTION_RULES = {"提交验证": "待验证", "启动合并": "运行中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "stream_batch_merge"
    UPSTREAM_MODULES = ["batch_slice", "stream_lookup"]
    PRODUCES_ARTIFACT = "流批合并记录"

    # 已处理的键值对版本追踪（防冲突）
    _merge_versions: dict[str, str] = {}
    # 计划编号 -> 最近一次发布清单
    _merge_manifests: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 绑定输入
    # ------------------------------------------------------------------
    def _batch_service(self):
        # 延迟导入，避免模块初始化阶段的循环依赖。
        from app.services import service_for

        return service_for("batch_slice")

    def _bound_batches(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """找到与计划绑定的同一源表、同一键列批次。"""
        batch_svc = self._batch_service()
        if batch_svc is None:
            return []

        batch_no = str(entry.get("批次编号", "") or "").strip()
        source_table = str(entry.get("批式来源", "") or "").strip()
        merge_key = str(entry.get("合并键列", "") or "").strip()
        batches = store.rows("batch_slice")

        if batch_no:
            explicit = [row for row in batches if str(row.get("批次编号", "")) == batch_no]
            if explicit:
                return explicit

        exact = []
        for batch in batches:
            if str(batch.get("源表名称", "") or "").strip() != source_table:
                continue
            if merge_key and str(batch.get("分片键列", "") or "").strip() != merge_key:
                continue
            exact.append(batch)
        return exact

    def check_readiness(self, entry_id: int) -> dict[str, Any]:
        """同步并返回计划绑定批次的完整性结论。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "publishable": False, "reason": f"计划 {entry_id} 不存在"}

        batch_svc = self._batch_service()
        bound = self._bound_batches(entry)
        reports = []
        for batch in bound:
            reports.append(batch_svc.evaluate_completeness(batch, mutate=True))

        if not bound:
            # 未显式绑定到具体批次规格时保留旧版全局流水线兼容路径。
            ready = True
            complete = True
            conclusion = "未绑定具体批次规格，按兼容模式读取流水线制品"
            remaining = 0
            missing = 0
            dirty = 0
        else:
            complete = all(report["complete"] for report in reports)
            source_ready = all(report["source_ready"] for report in reports)
            ready = complete and source_ready
            remaining = sum(int(report.get("remaining_rows", 0)) for report in reports)
            missing = sum(int(report.get("missing_rows", 0)) for report in reports)
            dirty = sum(int(report.get("dirty_row_count", report.get("dirty_rows", 0))) for report in reports)
            if ready:
                conclusion = "绑定批次的源表、键列、区间和流水线输入完整，允许发布"
            else:
                parts = []
                if not complete:
                    parts.append(f"剩余 {remaining} 行待补齐（区间未核对位置 {missing} 个）")
                if not source_ready:
                    parts.append("可用流式源未确认")
                conclusion = "；".join(parts) + "，暂不允许发布"

        result = {
            "ok": ready,
            "complete": complete,
            "publishable": ready,
            "can_publish": ready,
            "remaining_rows": remaining,
            "remaining": remaining,
            "remaining_count": remaining,
            "missing_rows": missing,
            "dirty_rows": dirty,
            "dirty_row_count": dirty,
            "bound_batch_count": len(bound),
            "bound_batches": [
                {
                    "entry_id": batch.get("id"),
                    "batch_no": batch.get("批次编号"),
                    "status": batch.get("status"),
                    "remaining_rows": report.get("remaining_rows"),
                    "missing_rows": report.get("missing_rows"),
                    "publishable": report.get("publishable"),
                    "conclusion": report.get("conclusion"),
                }
                for batch, report in zip(bound, reports)
            ],
            "reports": reports,
            "conclusion": conclusion,
            "完整性结论": conclusion,
        }
        entry["完整性结论"] = conclusion
        entry["completeness"] = complete
        entry["publishable"] = ready
        entry["remaining_rows"] = remaining
        entry["missing_rows"] = missing
        entry["剩余工作"] = (
            "无剩余工作" if ready else f"剩余 {remaining} 行待补齐（{missing} 个区间位置待核对）"
        ) if bound else "兼容模式：未绑定具体批次规格"
        if bound:
            entry["abnormal"] = not ready
            entry["pending"] = not ready
        return result

    # ------------------------------------------------------------------
    # 合并执行
    # ------------------------------------------------------------------
    def execute_merge(
        self,
        entry_id: int,
        *,
        publish: bool = True,
        stream_rows: list[dict[str, Any]] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """执行流批合并：消费上游 batch_slice 和 stream_lookup 的数据产物。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}

        current_status = str(entry.get("status", ""))
        already_published = bool(entry.get("published")) and entry.get("published_fingerprint")
        if current_status != "运行中" and not (current_status == "已完成" and already_published):
            return {"ok": False, "reason": f"计划未运行，当前状态: {entry.get('status')}"}

        merge_key = str(entry.get("合并键列", "id"))
        conflict_policy = str(entry.get("冲突解决", "stream_win"))
        dedup_policy = str(entry.get("去重策略", "unique"))

        readiness = self.check_readiness(entry_id)
        bound_batches = self._bound_batches(entry)
        batch_svc = self._batch_service()

        # 读取批式输入。绑定规格时只读取本计划批次，避免全局制品污染验收输入。
        if bound_batches:
            batch_rows: list[dict[str, Any]] = []
            partial_rows: list[dict[str, Any]] = []
            for batch in bound_batches:
                rows = batch_svc.actual_input_rows(int(batch["id"]))
                partial_rows.extend(rows)
                if readiness["publishable"]:
                    batch_rows.extend(rows)
        else:
            batch_pl = store.pipeline("batch_slicing")
            partial_rows = list(batch_pl._artifacts[:30] if batch_pl else [])
            batch_rows = partial_rows

        lookup_pl = store.pipeline("stream_lookup_join")
        all_stream_rows = list(lookup_pl._artifacts[:30] if lookup_pl else [])
        if stream_rows is not None:
            stream_rows = list(stream_rows)
        elif bound_batches:
            source_ref = str(entry.get("流式来源", "") or "")
            scoped = [
                row for row in all_stream_rows
                if row.get("source_identifier") == source_ref
                or row.get("source") == source_ref
                or row.get("左流来源") == source_ref
            ]
            # 新写入的关联结果带来源标记；没有标记时不把其他计划的全局制品
            # 误当成本计划实际输入。
            stream_rows = scoped
        else:
            stream_rows = all_stream_rows

        # 中断批次：合并入口必须反映剩余工作，且不能发布不完整制品。
        if bound_batches and not readiness["publishable"]:
            reason = f"合并计划 {entry.get('计划编号')} 输入不完整：{readiness['conclusion']}"
            result = {
                "ok": False,
                "execution_ok": True,
                "published": False,
                "published_rows": 0,
                "publishable": False,
                "can_publish": False,
                "completeness": False,
                "code": "INPUT_INCOMPLETE",
                "status_code": 409,
                "reason": reason,
                "entry_id": entry_id,
                "batch_rows_consumed": len(partial_rows),
                "available_batch_rows": len(partial_rows),
                "admitted_batch_rows": 0,
                "stream_rows_consumed": len(stream_rows),
                "remaining_rows": readiness["remaining_rows"],
                "remaining": readiness["remaining_rows"],
                "remaining_count": readiness["remaining_rows"],
                "missing_rows": readiness["missing_rows"],
                "bound_batch_count": readiness.get("bound_batch_count", len(bound_batches)),
                "剩余工作": entry.get("剩余工作"),
                "dirty_rows": readiness.get("dirty_rows", 0),
                "dirty_row_count": readiness.get("dirty_row_count", 0),
                "bound_batches": readiness["bound_batches"],
                "completeness_report": readiness,
                "actual_input": partial_rows,
                "batch_input": partial_rows,
                "input_rows": partial_rows,
                "stream_input": stream_rows,
                "actual_inputs": {
                    "batch_rows": partial_rows,
                    "admitted_batch_rows": [],
                    "stream_rows": stream_rows,
                },
                "完整性结论": readiness["conclusion"],
            }
            entry["last_merge_result"] = {
                key: value for key, value in result.items()
                if key not in {"actual_input", "batch_input", "stream_input", "actual_inputs", "completeness_report"}
            }
            return result

        # 合并去重逻辑
        merged: dict[Any, dict[str, Any]] = {}
        conflicts = []

        for row in batch_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflict = {"key": key, "existing": merged[key], "new": row, "type": "batch_batch_conflict"}
                conflicts.append(conflict)
                if conflict_policy == "stream_win":
                    continue
            merged[key] = row

        for row in stream_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflict = {"key": key, "existing": merged[key], "new": row, "type": "stream_batch_conflict"}
                conflicts.append(conflict)
                if conflict_policy == "stream_win":
                    merged[key] = row  # 流式优先覆盖
            else:
                merged[key] = row

        result_rows = list(merged.values())

        # 去重：根据去重策略
        if dedup_policy == "unique":
            seen = set()
            deduped = []
            for row in result_rows:
                key = row.get(merge_key)
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            result_rows = deduped

        fingerprint_source = json.dumps(
            {
                "merge_key": merge_key,
                "batch": sorted(
                    [
                        str(row.get("batch_no") or row.get("batch_id") or ""),
                        str(row.get("row_index") or row.get("id") or ""),
                        str(row.get("value") or ""),
                    ]
                    for row in batch_rows
                ),
                "stream_count": len(stream_rows),
                "result_keys": sorted(str(row.get(merge_key)) for row in result_rows),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        fingerprint = hashlib.md5(fingerprint_source.encode("utf-8")).hexdigest()[:12]
        plan_no = str(entry.get("计划编号", entry_id))
        publish_batch_id = f"MERGE_{plan_no}_{fingerprint}"

        if entry.get("published_fingerprint") == fingerprint:
            published_rows = int(entry.get("published_rows", len(result_rows)))
            idempotent_result = {
                "ok": True,
                "duplicate": True,
                "idempotent": True,
                "published": True,
                "publishable": True,
                "entry_id": entry_id,
                "batch_rows_consumed": len(batch_rows),
                "stream_rows_consumed": len(stream_rows),
                "merged_result": len(result_rows),
                "published_rows": published_rows,
                "conflicts": len(conflicts),
                "conflict_samples": conflicts[:3],
                "fingerprint": fingerprint,
                "actual_input": batch_rows,
                "batch_input": batch_rows,
                "input_rows": batch_rows,
                "stream_input": stream_rows,
                "actual_inputs": {"batch_rows": batch_rows, "stream_rows": stream_rows},
                "reason": "相同输入已发布，重试未创建另一份成功结果",
            }
            return idempotent_result

        write_result = None
        if publish:
            write_result = self.produce_artifact(result_rows, publish_batch_id, replace=False)
            if not write_result.get("ok") and not write_result.get("duplicate"):
                return {
                    "ok": False,
                    "reason": f"合并制品写入失败：{write_result}",
                    "write_result": write_result,
                }

        manifest = {
            "plan_id": entry_id,
            "plan_no": plan_no,
            "fingerprint": fingerprint,
            "publish_batch_id": publish_batch_id,
            "published": bool(publish),
            "published_rows": len(result_rows),
            "batch_rows_consumed": len(batch_rows),
            "stream_rows_consumed": len(stream_rows),
            "merged_result": len(result_rows),
            "conflicts": len(conflicts),
            "dirty_rows": readiness.get("dirty_rows", 0),
            "bound_batches": readiness.get("bound_batches", []),
            "dirty_details": [
                dirty
                for report in readiness.get("reports", [])
                for dirty in report.get("dirty_rows_detail", [])
            ],
            "actual_batch_input": [
                {
                    "batch_no": row.get("batch_no") or row.get("batch_id"),
                    "row_index": row.get("row_index"),
                    "id": row.get("id"),
                    "value": row.get("value"),
                    "source_table": row.get("source_table"),
                    "slice_key": row.get("slice_key"),
                }
                for row in batch_rows
            ],
            "published_at": time.time(),
            "完整性结论": readiness["conclusion"],
        }
        self._merge_manifests[plan_no] = manifest
        entry["merge_manifest"] = manifest
        entry["published"] = bool(publish)
        entry["published_fingerprint"] = fingerprint if publish else entry.get("published_fingerprint")
        entry["published_rows"] = len(result_rows)
        entry["完整性结论"] = readiness["conclusion"] + f"；本次发布 {len(result_rows)} 行"
        entry["last_merge_result"] = {
            "ok": True,
            "published": bool(publish),
            "batch_rows_consumed": len(batch_rows),
            "stream_rows_consumed": len(stream_rows),
            "merged_result": len(result_rows),
            "conflicts": len(conflicts),
            "fingerprint": fingerprint,
        }

        return {
            "ok": True,
            "published": bool(publish),
            "publishable": True,
            "entry_id": entry_id,
            "batch_rows_consumed": len(batch_rows),
            "stream_rows_consumed": len(stream_rows),
            "merged_result": len(result_rows),
            "published_rows": len(result_rows) if publish else 0,
            "conflicts": len(conflicts),
            "conflict_samples": conflicts[:3],
            "dirty_rows": readiness.get("dirty_rows", 0),
            "dirty_row_count": readiness.get("dirty_row_count", 0),
            "fingerprint": fingerprint,
            "write_result": write_result,
            "actual_input": batch_rows,
            "batch_input": batch_rows,
            "input_rows": batch_rows,
            "stream_input": stream_rows,
            "actual_inputs": {"batch_rows": batch_rows, "stream_rows": stream_rows},
            "completeness": True,
            "completeness_report": readiness,
            "完整性结论": entry["完整性结论"],
        }

    # 常见入口别名。
    def run_merge(self, entry_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.execute_merge(entry_id, **kwargs)

    merge = execute_merge

    def publish_merge(self, entry_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.execute_merge(entry_id, publish=True, **kwargs)

    def check_completeness(self, entry_id: int) -> dict[str, Any]:
        return self.check_readiness(entry_id)

    def merge_manifest(self, entry_id_or_no: int | str) -> dict[str, Any] | None:
        if isinstance(entry_id_or_no, str):
            return self._merge_manifests.get(entry_id_or_no)
        entry = store.find(self.MODULE, entry_id_or_no)
        if entry is None:
            return None
        return entry.get("merge_manifest") or self._merge_manifests.get(str(entry.get("计划编号")))

    get_merge_manifest = merge_manifest

    def list_merge_manifests(self) -> list[dict[str, Any]]:
        return list(self._merge_manifests.values())

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action in {"启动合并", "执行合并", "发布合并", "重跑合并"}:
            # 「启动合并」先完成既有的状态流转，再进入真实合并入口。
            if action == "启动合并" and entry.get("status") != "运行中":
                entry["status"] = "运行中"
                entry["pending"] = False
            result = self.execute_merge(entry_id)
            if result.get("ok"):
                entry["_action_ok"] = True
                return entry, f"{self.ENTITY}已{action}，发布 {result.get('published_rows', 0)} 行"
            entry["_action_ok"] = False
            return entry, str(result.get("reason") or "合并输入不完整")
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_plans"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "运行中")
        base["published_plans"] = sum(1 for r in store.rows(self.MODULE) if r.get("published"))
        return base
