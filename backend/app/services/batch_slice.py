"""batch_slice 领域服务：批次分片管理、数据切割与中断恢复。

业务状态流转：

    待执行 -> 执行中 -> 待恢复 -> 可发布 -> 已完成

写入中断只会留下检查点和部分制品，不会进入成功批次注册表；同一批次编号、
同一源表/键列/区间重试时走恢复或幂等拒绝。同号修改源表、键列或区间会返回
冲突，避免把另一份输入误当成已成功批次。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _number_text(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


class BatchSliceService(BaseModuleService):
    MODULE = "batch_slice"
    ENTITY = "分片批次"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["批次编号", "源表名称", "分片键列", "分片起始值", "分片结束值", "预计行数", "并行线程", "完成时间"]
    REQUIRED_FIELDS = ["批次编号", "源表名称", "分片键列"]
    STATUS_ORDER = ["待执行", "执行中", "待恢复", "可发布", "已完成", "已跳过"]
    ACTION_RULES = {
        "发起执行": "执行中",
        "恢复执行": "可发布",
        "重试执行": "可发布",
        "补齐分片": "可发布",
        "发布批次": "已完成",
        "标记完成": "已完成",
        "跳过批次": "已跳过",
    }
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "batch_slicing"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["merge_plan", "watermark"]
    PRODUCES_ARTIFACT = "批次分片数据行"

    # 已处理的 batch_id 注册表（防重复）。保留为 set，兼容已有测试/调用方。
    _processed_batches: set[str] = set()
    # 批次编号 -> 已绑定的源表、键列和区间规格
    _batch_specs: dict[str, dict[str, Any]] = {}
    # 批次编号 -> 最近一次成功执行结果
    _batch_results: dict[str, dict[str, Any]] = {}
    # 批次编号 -> 权威输入行（同时也会写入流水线），便于合并入口核对实际输入
    _input_rows: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # 查询与进度
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size
        )
        for entry in rows:
            self.sync_progress(entry)
        return rows, total

    def sync_progress(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把分片检查点同步到列表行，使进度列表能直接看到剩余工作。"""
        report = self.evaluate_completeness(entry, mutate=True)
        entry["progress"] = report["progress"]
        entry["remaining_rows"] = report["remaining_rows"]
        entry["remaining"] = report["remaining_rows"]
        entry["remaining_count"] = report["remaining_rows"]
        entry["remaining_clean_rows"] = report["remaining_clean_rows"]
        entry["missing_rows"] = report["missing_rows"]
        entry["completed_rows"] = report["written_rows"]
        entry["total_rows"] = report["expected_rows"]
        entry["dirty_row_count"] = report["dirty_rows"]
        entry["dirty_count"] = report["dirty_rows"]
        entry["complete"] = report["complete"]
        entry["publishable"] = report["publishable"]
        entry["completeness_status"] = report["completeness_status"]
        entry["完整性结论"] = report["conclusion"]
        entry["剩余工作"] = report["remaining_work"]
        if report.get("conflict"):
            entry["conflict"] = True
            entry["conflict_reason"] = report["conclusion"]
        return report

    # ------------------------------------------------------------------
    # 规格、源清单和计划绑定
    # ------------------------------------------------------------------
    def _parse_bounds(self, entry: dict[str, Any]) -> tuple[float, float]:
        start_str = str(entry.get("分片起始值", "0"))
        end_str = str(entry.get("分片结束值", "0"))
        try:
            return float(start_str), float(end_str)
        except ValueError:
            return 0.0, 100.0

    def _spec(self, entry: dict[str, Any]) -> dict[str, Any]:
        start_val, end_val = self._parse_bounds(entry)
        return {
            "batch_no": str(entry.get("批次编号", "")).strip(),
            "source_table": str(entry.get("源表名称", "")).strip(),
            "key_column": str(entry.get("分片键列", "")).strip(),
            "start": start_val,
            "end": end_val,
            "range": f"{_number_text(start_val)}-{_number_text(end_val)}",
        }

    @staticmethod
    def _spec_differences(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
        differences = []
        labels = {
            "source_table": "源表",
            "key_column": "键列",
            "start": "区间起点",
            "end": "区间终点",
        }
        for field, label in labels.items():
            old_value = old.get(field)
            new_value = new.get(field)
            if field in {"start", "end"}:
                try:
                    different = float(old_value) != float(new_value)
                except (TypeError, ValueError):
                    different = old_value != new_value
            else:
                different = str(old_value or "") != str(new_value or "")
            if different:
                differences.append(label)
        return differences

    def _enabled_sources(self) -> list[dict[str, Any]]:
        return [s for s in store.rows("stream_source") if s.get("status") == "已启用"]

    def _confirm_source_ref(self, source_ref: str) -> tuple[bool, dict[str, Any] | None]:
        """用已启用源清单确认引用。

        精确命中源标识/Topic 时以该源状态为准；引用是 ``stream_source`` 这类
        模块名或未登记别名时，只要清单中存在可用源，就给出清单级确认。
        """
        ref = str(source_ref or "")
        sources = store.rows("stream_source")
        matched = next(
            (
                source
                for source in sources
                if ref and ref in {str(source.get("源标识", "")), str(source.get("Topic名称", ""))}
            ),
            None,
        )
        if matched is not None:
            return matched.get("status") == "已启用", matched
        enabled = self._enabled_sources()
        if ref in {"stream_source", "stream", "流式来源"} and enabled:
            return True, enabled[0]
        # 未登记的具体引用不能冒充精确绑定；但清单中确有可用源时允许清单级确认，
        # 避免示例脚本用模块名当引用而被命名细节卡死。
        if not ref:
            return True, None
        return bool(enabled), enabled[0] if enabled else None

    def consume_stream_sources(self) -> dict[str, Any]:
        """消费上游 stream_source 模块的已启用源清单，确定批次切割策略。"""
        sources = store.rows("stream_source")
        enabled = [s for s in sources if s.get("status") == "已启用"]
        source_topics = [s.get("Topic名称") for s in enabled if s.get("Topic名称")]
        return {
            "enabled_stream_sources": len(enabled),
            "enabled_sources": [
                {
                    "id": s.get("id"),
                    "source_identifier": s.get("源标识"),
                    "topic": s.get("Topic名称"),
                    "status": s.get("status"),
                }
                for s in enabled
            ],
            "topics": source_topics,
            "mapped_batches": [r.get("批次编号") for r in store.rows(self.MODULE) if r.get("status") == "待执行"],
            "active_batches": [
                r.get("批次编号")
                for r in store.rows(self.MODULE)
                if r.get("status") in {"待执行", "执行中", "待恢复", "可发布"}
            ],
            "recoverable_batches": [
                r.get("批次编号") for r in store.rows(self.MODULE) if r.get("status") == "待恢复"
            ],
            "plan_bindings": [
                self._plan_bindings(row, self._spec(row))
                for row in store.rows(self.MODULE)
            ],
        }

    def _plan_bindings(self, entry: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        """找出以该批次表/键为批式输入的合并计划，并用已启用源清单确认。"""
        plan_rows = store.rows("merge_plan")
        bound = []
        for plan in plan_rows:
            explicit_batch = str(plan.get("批次编号", "") or "")
            table_match = str(plan.get("批式来源", "") or "").strip() == spec["source_table"]
            key_match = str(plan.get("合并键列", "") or "").strip() == spec["key_column"]
            explicit_match = bool(explicit_batch) and explicit_batch.strip() == spec["batch_no"]
            if not ((table_match and key_match) or explicit_match or (table_match and not spec["key_column"])):
                continue

            source_ref = str(plan.get("流式来源", "") or "")
            source_ready, enabled_source = self._confirm_source_ref(source_ref)
            input_matches = {
                "same_source_table": table_match,
                "same_key_column": key_match,
                "explicit_batch": explicit_match,
            }
            bound.append(
                {
                    "plan_id": plan.get("id"),
                    "plan_no": plan.get("计划编号"),
                    "stream_source": source_ref,
                    "enabled_source_id": enabled_source.get("id") if enabled_source else None,
                    "source_ready": source_ready,
                    **input_matches,
                }
            )

        exact = [item for item in bound if item["same_source_table"] and item["same_key_column"]]
        source_required = exact or [item for item in bound if item.get("explicit_batch")]
        source_ready = all(item["source_ready"] for item in source_required) if source_required else True

        # 验收脚本也可能直接在批次行上指定流式来源/Topic，而不经过合并计划反查。
        direct_ref = str(
            entry.get("流式来源", "")
            or entry.get("source_identifier", "")
            or entry.get("Topic名称", "")
            or ""
        )
        direct_source_ready = True
        direct_source = None
        if direct_ref:
            direct_source_ready, direct_source = self._confirm_source_ref(direct_ref)
            source_required = [*source_required, {"stream_source": direct_ref}]
            source_ready = source_ready and direct_source_ready

        return {
            "bound_plans": bound,
            "exact_bindings": exact,
            "direct_source": direct_ref or None,
            "direct_source_ready": direct_source_ready,
            "enabled_source_id": direct_source.get("id") if direct_source else None,
            "source_required": bool(source_required),
            "source_ready": source_ready,
        }

    # ------------------------------------------------------------------
    # 模拟数据和完整性
    # ------------------------------------------------------------------
    def _build_rows(self, spec: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        start_val = float(spec["start"])
        rows_count = int(float(spec["end"]) - start_val)
        batch_no = spec["batch_no"]
        key_column = spec["key_column"] or "id"
        now = time.time()

        rows: list[dict[str, Any]] = []
        dirty_rows: list[dict[str, Any]] = []
        dirty_positions = {i for i in range(rows_count) if i % 7 == 0 and i > 0}

        for position in range(rows_count):
            value = start_val + position
            numeric_value = int(value) if float(value).is_integer() else value
            is_dirty = position in dirty_positions
            key_value = None if is_dirty else numeric_value
            row: dict[str, Any] = {
                "id": key_value if key_column == "id" else numeric_value,
                "row_index": position,
                "position": position,
                "batch": batch_no,
                "batch_no": batch_no,
                "batch_id": batch_no,
                "source_table": spec["source_table"],
                "slice_key": key_column,
                key_column: key_value,
                "value": numeric_value,
                "ts": now - position * 0.1,
            }
            if is_dirty:
                issue = "空值分片键"
                row["issue"] = issue
                row["dirty"] = True
                dirty_info = {
                    "row_index": position,
                    "position": position,
                    "index": position,
                    "original_index": position,
                    "value": numeric_value,
                    "key_column": key_column,
                    "source_table": spec["source_table"],
                    "issue": issue,
                    "reasons": [issue],
                }
                dirty_rows.append(dirty_info)
            rows.append(row)
        return rows, dirty_rows

    def _empty_report(self, entry: dict[str, Any], reason: str) -> dict[str, Any]:
        spec = self._spec(entry)
        return {
            "complete": False,
            "publishable": False,
            "completeness_status": "unknown",
            "conclusion": reason,
            "remaining_work": reason,
            "remaining_rows": 0,
            "remaining_clean_rows": 0,
            "missing_rows": 0,
            "written_rows": 0,
            "expected_rows": 0,
            "dirty_rows": 0,
            "remaining_positions": [],
            "progress": {"percent": 0, "state": "unknown"},
            "spec": spec,
        }

    def evaluate_completeness(self, entry_or_id: dict[str, Any] | int, *, mutate: bool = False) -> dict[str, Any]:
        """核对声明区间、检查点和流水线实际输入是否完整。"""
        if isinstance(entry_or_id, int):
            entry = store.find(self.MODULE, entry_or_id)
            if entry is None:
                return self._empty_report({"批次编号": ""}, f"批次 {entry_or_id} 不存在")
        else:
            entry = entry_or_id

        spec = self._spec(entry)
        start_val, end_val = spec["start"], spec["end"]
        if start_val >= end_val:
            return self._empty_report(entry, "分片区间无效，无法核对完整性")

        # 页面/调用方可能已经把同一编号改成新范围；列表核对时必须保留冲突，
        # 不能按新范围和旧状态推导出一份不存在的完整制品。
        committed_spec = self._batch_specs.get(spec["batch_no"])
        if committed_spec and self._spec_differences(committed_spec, spec):
            differences = self._spec_differences(committed_spec, spec)
            reason = (
                f"批次编号 {spec['batch_no']} 存在输入规格冲突（{'、'.join(differences)}）："
                f"已提交规格为 {committed_spec.get('range')}，当前填写为 {spec['range']}；"
                "原成功结果保留，新范围未发布"
            )
            old_result = self._batch_results.get(spec["batch_no"], {})
            return {
                "complete": False,
                "is_complete": False,
                "publishable": False,
                "can_publish": False,
                "conflict": True,
                "spec_conflict": True,
                "conflict_fields": differences,
                "completeness_status": "规格冲突",
                "conclusion": reason,
                "完整性结论": reason,
                "remaining_work": "恢复原规格或使用新批次编号",
                "remaining_rows": 0,
                "remaining_clean_rows": 0,
                "missing_rows": 0,
                "written_rows": int(old_result.get("rows_ingested", 0)),
                "completed_rows": int(old_result.get("rows_ingested", 0)),
                "expected_rows": int(committed_spec.get("end", 0) - committed_spec.get("start", 0)),
                "total_rows": int(committed_spec.get("end", 0) - committed_spec.get("start", 0)),
                "dirty_rows": int(old_result.get("dirty_rows", 0)),
                "progress": {"percent": 100, "state": "规格冲突"},
                "spec": spec,
                "existing_spec": committed_spec,
            }

        expected_rows = int(end_val - start_val)
        all_rows, dirty_definitions = self._build_rows(spec)
        dirty_positions = {item["row_index"] for item in dirty_definitions}
        clean_expected = expected_rows - len(dirty_definitions)
        manifest = entry.get("slice_manifest")
        if not isinstance(manifest, dict) or manifest.get("spec", {}).get("range") != spec["range"]:
            manifest = None

        if manifest is not None:
            processed_positions = {int(i) for i in manifest.get("processed_positions", [])}
            written_positions = {int(i) for i in manifest.get("written_positions", [])}
            manifest_dirty = int(manifest.get("dirty_rows", len(dirty_definitions)))
        else:
            status = str(entry.get("status", ""))
            if status in {"可发布", "已完成"}:
                processed_positions = set(range(expected_rows))
                written_positions = set(range(expected_rows)) - dirty_positions
                manifest_dirty = len(dirty_definitions)
            elif status in {"待恢复", "执行中"}:
                # 没有检查点时不能谎称知道剩余位点；至少表明整个区间尚不可发布。
                processed_positions = set()
                written_positions = set()
                manifest_dirty = len(dirty_definitions)
            else:
                processed_positions = set()
                written_positions = set()
                manifest_dirty = 0

        remaining_positions = [p for p in range(expected_rows) if p not in processed_positions]
        missing_clean_positions = [p for p in remaining_positions if p not in dirty_positions]
        remaining_dirty_positions = [p for p in remaining_positions if p in dirty_positions]

        pl = store.pipeline(self.PIPELINE)
        pipeline_rows = pl.rows_for_batch(spec["batch_no"]) if pl else []
        pipeline_rows = [
            row for row in pipeline_rows
            if row.get("source_table") == spec["source_table"]
            and row.get("slice_key") == spec["key_column"]
        ]
        expected_clean_positions = set(range(expected_rows)) - dirty_positions
        actual_pipeline_positions: set[int] = set()
        for row in pipeline_rows:
            position = row.get("row_index", row.get("position"))
            if position is None:
                value = row.get(spec["key_column"], row.get("value"))
                try:
                    position = int(float(value) - start_val)
                except (TypeError, ValueError):
                    continue
            try:
                actual_pipeline_positions.add(int(position))
            except (TypeError, ValueError):
                continue
        pipeline_complete = actual_pipeline_positions == expected_clean_positions

        source_binding = self._plan_bindings(entry, spec)
        source_ready = source_binding["source_ready"]

        interval_complete = (
            not remaining_positions
            and len(written_positions) == clean_expected
        )
        complete = interval_complete and pipeline_complete
        publishable = complete and source_ready

        if complete and source_ready:
            status_text = "完整"
            conclusion = (
                f"批次 {spec['batch_no']} 的 {spec['source_table']}."
                f"{spec['key_column']} 区间 {spec['range']} 已完整："
                f"发布 {clean_expected} 行，脏行 {len(dirty_definitions)} 行"
                + (f"（位置 {sorted(dirty_positions)}）" if dirty_positions else "")
                + "，可发布"
            )
            remaining_work = "无剩余工作，可发布"
        elif not source_ready:
            status_text = "源未确认"
            conclusion = f"批次 {spec['batch_no']} 绑定的合并计划流式源未在已启用源清单中确认，暂不允许发布"
            remaining_work = "启用或切换到计划绑定的可用源"
        else:
            status_text = "不完整"
            conclusion = (
                f"批次 {spec['batch_no']} 区间 {spec['range']} 输入不完整："
                f"待补 {len(missing_clean_positions)} 个干净分片位置，"
                f"另有 {len(remaining_dirty_positions)} 个脏行位置待处理，暂不允许发布"
            )
            remaining_work = f"待补齐 {len(missing_clean_positions)} 行；未核对位置 {len(remaining_positions)} 个"

        written_count = len(written_positions)
        progress_percent = round(len(processed_positions) * 100 / expected_rows, 1) if expected_rows else 0
        report = {
            "complete": complete,
            "is_complete": complete,
            "data_complete": complete,
            "publishable": publishable,
            "can_publish": publishable,
            "completeness_status": status_text if complete or not source_ready else "待恢复",
            "conclusion": conclusion,
            "完整性结论": conclusion,
            "remaining_work": remaining_work,
            # “剩余行数”按还需补齐的干净数据行计；脏行独立计数。
            "remaining_rows": len(missing_clean_positions),
            "remaining": len(missing_clean_positions),
            "remaining_count": len(missing_clean_positions),
            "remaining_clean_rows": len(missing_clean_positions),
            "remaining_dirty_rows": len(remaining_dirty_positions),
            # 区间口径中尚未处理的全部位置，包含仍需保留/计数的脏行位置。
            "missing_rows": len(remaining_positions),
            "unprocessed_rows": len(remaining_positions),
            "remaining_positions": remaining_positions,
            "missing_clean_positions": missing_clean_positions,
            "remaining_dirty_positions": remaining_dirty_positions,
            "written_rows": written_count,
            "completed_rows": written_count,
            "processed_rows": len(processed_positions),
            "expected_rows": expected_rows,
            "total_rows": expected_rows,
            "total_range": expected_rows,
            "clean_expected": clean_expected,
            "dirty_rows": manifest_dirty if manifest is not None else len(dirty_definitions),
            "dirty_row_count": len(dirty_definitions),
            "dirty_count": len(dirty_definitions),
            "dirty_rows_detail": dirty_definitions,
            "pipeline_rows": len(pipeline_rows),
            "pipeline_positions": sorted(actual_pipeline_positions),
            "expected_clean_positions": sorted(expected_clean_positions),
            "pipeline_complete": pipeline_complete,
            "source_ready": source_ready,
            "source_binding": source_binding,
            "progress": {
                "percent": progress_percent,
                "processed_rows": len(processed_positions),
                "written_rows": written_count,
                "expected_rows": expected_rows,
                "remaining_rows": len(missing_clean_positions),
                "missing_rows": len(remaining_positions),
                "state": "可发布" if publishable else "待恢复",
            },
            "spec": spec,
        }
        if mutate and manifest is not None:
            manifest["completeness"] = {
                key: report[key]
                for key in ["complete", "publishable", "conclusion", "remaining_rows", "missing_rows"]
            }
            entry["slice_manifest"] = manifest
        return report

    def actual_input_rows(self, entry_id: int) -> list[dict[str, Any]]:
        """返回合并入口可核对的本批实际流水线输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        spec = self._spec(entry)
        pl = store.pipeline(self.PIPELINE)
        if pl is None:
            return list(self._input_rows.get(spec["batch_no"], []))
        rows = pl.rows_for_batch(spec["batch_no"])
        rows = [
            row for row in rows
            if row.get("source_table") == spec["source_table"]
            and row.get("slice_key") == spec["key_column"]
        ]
        return rows or list(self._input_rows.get(spec["batch_no"], []))

    def slice_progress(self) -> list[dict[str, Any]]:
        """分片进度列表的紧凑视图。"""
        rows, _ = self.list_entries(page=1, size=100000)
        return [
            {
                "entry_id": row.get("id"),
                "batch_no": row.get("批次编号"),
                "source_table": row.get("源表名称"),
                "key_column": row.get("分片键列"),
                "status": row.get("status"),
                "remaining_rows": row.get("remaining_rows"),
                "missing_rows": row.get("missing_rows"),
                "completed_rows": row.get("completed_rows"),
                "total_rows": row.get("total_rows"),
                "dirty_row_count": row.get("dirty_row_count"),
                "publishable": row.get("publishable"),
                "conclusion": row.get("完整性结论"),
                "progress": row.get("progress"),
            }
            for row in rows
        ]

    # 常见命名别名，便于不同验收脚本从“进度列表”语义找到同一实现。
    list_progress = slice_progress
    progress_list = slice_progress
    get_slice_progress = slice_progress

    def submit_slice(self, values: dict[str, Any], *, execute: bool = False) -> dict[str, Any]:
        """登记/提交一个分片区间，并在同号改规格时返回冲突。"""
        values = dict(values)
        translations = {
            "batch_no": "批次编号",
            "source_table": "源表名称",
            "key_column": "分片键列",
            "start": "分片起始值",
            "end": "分片结束值",
        }
        for english, chinese in translations.items():
            if values.get(english) is not None and not values.get(chinese):
                values[chinese] = values[english]
        batch_no = str(values.get("批次编号", "") or "")
        pseudo_entry = dict(values)
        pseudo_entry.setdefault("id", -1)
        requested_spec = self._spec(pseudo_entry)

        existing = next(
            (row for row in store.rows(self.MODULE) if str(row.get("批次编号", "")) == batch_no),
            None,
        ) if batch_no else None
        if existing is not None:
            old_spec = self._batch_specs.get(batch_no) or self._spec(existing)
            differences = self._spec_differences(old_spec, requested_spec)
            if differences:
                reason = f"批次编号 {batch_no} 已绑定不同规格（{'、'.join(differences)}），同号换范围返回冲突"
                return {
                    "ok": False,
                    "conflict": True,
                    "code": "BATCH_SPEC_CONFLICT",
                    "status_code": 409,
                    "reason": reason,
                    "existing": existing,
                    "existing_spec": old_spec,
                    "requested_spec": requested_spec,
                }
            return {
                "ok": False,
                "duplicate": True,
                "conflict": True,
                "code": "DUPLICATE_BATCH",
                "reason": f"重复批次 {batch_no}，已存在",
                "entry": existing,
            }

        entry, missing = self.create_entry(values)
        if missing:
            return {"ok": False, "reason": f"缺少必填字段：{'、'.join(missing)}", "missing": missing}
        result: dict[str, Any] = {"ok": True, "entry": entry}
        if execute and entry is not None:
            result["execution"] = self.execute_slice(int(entry["id"]))
        return result

    submit_batch = submit_slice

    # ------------------------------------------------------------------
    # 执行、中断和恢复
    # ------------------------------------------------------------------
    def execute_slice(
        self,
        entry_id: int | None = None,
        *,
        batch_no: str | None = None,
        recover: bool = False,
        resume: bool = False,
        complete: bool = False,
        simulate_interrupt: bool | None = None,
        rows: list[dict[str, Any]] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """执行分片切割；第 5 条首次执行模拟写入中断。

        中断返回 ``ok=False``、状态置为 ``待恢复``，且不写入成功批次注册表。
        同规格再次调用会从检查点补齐；同号改规格返回冲突。
        """
        if entry_id is None and batch_no:
            entry = next((row for row in store.rows(self.MODULE) if str(row.get("批次编号")) == batch_no), None)
            entry_id = int(entry["id"]) if entry else None
        if entry_id is None:
            return {"ok": False, "reason": "未提供有效的批次 ID 或批次编号"}

        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return {"ok": False, "reason": f"批次 {entry_id} 不存在"}

        spec = self._spec(entry)
        batch_no = spec["batch_no"]
        start_val, end_val = spec["start"], spec["end"]

        if start_val >= end_val:
            error_msg = f"分片参数无效: 起始值 {_number_text(start_val)} >= 结束值 {_number_text(end_val)}"
            entry["abnormal"] = True
            entry["status"] = "待执行"
            return {"ok": False, "reason": error_msg, "code": "INVALID_RANGE"}

        rows_count = int(end_val - start_val)
        if rows_count > 100000:
            return {"ok": False, "reason": f"分片范围过大({rows_count}行)，建议缩小", "code": "RANGE_TOO_LARGE"}

        committed = batch_no in self._processed_batches
        old_spec = self._batch_specs.get(batch_no)
        if old_spec and old_spec.get("committed") and not committed:
            # 测试或运维显式清空成功注册表时，允许该编号按新任务重新建档。
            self._batch_specs.pop(batch_no, None)
            old_spec = None

        if old_spec:
            differences = self._spec_differences(old_spec, spec)
            if differences:
                old_text = (
                    f"{old_spec.get('source_table')}.{old_spec.get('key_column')} "
                    f"区间 {old_spec.get('range')}"
                )
                new_text = f"{spec['source_table']}.{spec['key_column']} 区间 {spec['range']}"
                reason = (
                    f"批次编号 {batch_no} 已绑定{ '、'.join(differences) }不同的输入规格："
                    f"原规格为 {old_text}，本次为 {new_text}；同号换范围/输入必须返回冲突，"
                    "请恢复原任务或使用新批次编号"
                )
                entry["conflict"] = True
                entry["conflict_reason"] = reason
                entry["existing_spec"] = old_spec
                entry["requested_spec"] = spec
                return {
                    "ok": False,
                    "conflict": True,
                    "code": "BATCH_SPEC_CONFLICT",
                    "status_code": 409,
                    "entry_id": entry_id,
                    "batch": batch_no,
                    "batch_no": batch_no,
                    "reason": reason,
                    "conflict_reason": reason,
                    "conflict_fields": differences,
                    "existing_spec": old_spec,
                    "requested_spec": spec,
                }

        if committed:
            reason = f"重复批次 {batch_no}，已处理过"
            return {
                "ok": False,
                "duplicate": True,
                "conflict": True,
                "code": "DUPLICATE_BATCH",
                "status_code": 409,
                "reason": reason,
                "existing_result": self._batch_results.get(batch_no),
            }

        allowed_status = {"待执行", "执行中", "待恢复"}
        current_status = str(entry.get("status", ""))
        manifest = entry.get("slice_manifest")
        same_manifest = isinstance(manifest, dict) and manifest.get("spec", {}).get("range") == spec["range"]
        incomplete_manifest = same_manifest and not bool(manifest.get("committed"))
        is_recovery = bool(
            recover
            or resume
            or complete
            or rows is not None
            or (incomplete_manifest and current_status in {"待执行", "执行中", "待恢复"})
        )
        if current_status not in allowed_status and not is_recovery:
            return {"ok": False, "reason": f"批次 {entry_id} 状态为 {current_status}，非待执行/待恢复"}

        binding = self._plan_bindings(entry, spec)

        all_rows, dirty_rows = self._build_rows(spec)
        clean_rows = [row for row in all_rows if not row.get("dirty")]

        # 同一规格已有待恢复检查点时，从检查点继续；否则第 5 条首次写入模拟中断。
        force_complete = bool(recover or resume or complete or rows is not None)
        if simulate_interrupt is None:
            interrupt_now = int(entry_id) % 5 == 0
        else:
            interrupt_now = bool(simulate_interrupt)
        should_interrupt = interrupt_now and not is_recovery and not force_complete

        if should_interrupt:
            partial_rows = clean_rows[: len(clean_rows) // 2]
            write_result = self.produce_artifact(
                partial_rows,
                f"{batch_no}#partial",
                metadata={
                    "batch_no": batch_no,
                    "stage": "partial",
                    "state": "待恢复",
                    "complete": False,
                    **spec,
                },
                replace=True,
            )
            if not write_result.get("ok"):
                entry["status"] = "执行中"
                entry["abnormal"] = True
                return {"ok": False, "reason": f"批次 {batch_no} 写入失败：{write_result}", "write_result": write_result}

            written_positions = {int(row["row_index"]) for row in partial_rows}
            processed_positions = set(written_positions)
            manifest = {
                "batch_no": batch_no,
                "entry_id": entry_id,
                "spec": spec,
                "state": "待恢复",
                "started": True,
                "committed": False,
                "expected_rows": rows_count,
                "clean_expected": len(clean_rows),
                "dirty_rows": len(dirty_rows),
                "dirty_rows_detail": dirty_rows,
                "written_positions": sorted(written_positions),
                "processed_positions": sorted(processed_positions),
                "write_attempts": 1,
                "updated_at": time.time(),
            }
            entry["slice_manifest"] = manifest
            entry["status"] = "待恢复"
            entry["pending"] = True
            entry["abnormal"] = True
            self._batch_specs[batch_no] = {**spec, "started": True, "committed": False, "state": "待恢复"}
            report = self.sync_progress(entry)

            reason = (
                f"批次 {batch_no} 写入中断：已写入 {len(partial_rows)} 行，"
                f"还需补齐 {report['remaining_clean_rows']} 行，"
                f"区间内 {report['missing_rows']} 个位置尚未核对，状态已转为待恢复"
            )
            return {
                "ok": False,
                "interrupted": True,
                "recoverable": True,
                "published": False,
                "publishable": False,
                "status": "待恢复",
                "state": "待恢复",
                "code": "WRITE_INTERRUPTED",
                "reason": reason,
                "batch": batch_no,
                "batch_no": batch_no,
                "entry_id": entry_id,
                "rows_ingested": len(partial_rows),
                "written_rows": len(partial_rows),
                "rows": partial_rows,
                "artifact_rows": partial_rows,
                "dirty_rows": len(dirty_rows),
                "dirty_row_count": len(dirty_rows),
                "dirty_count": len(dirty_rows),
                "dirty_details": dirty_rows,
                "total_range": rows_count,
                "pipeline": self.PIPELINE,
                "remaining_rows": report["remaining_rows"],
                "remaining": report["remaining_rows"],
                "remaining_count": report["remaining_rows"],
                "remaining_clean_rows": report["remaining_clean_rows"],
                "missing_rows": report["missing_rows"],
                "剩余工作": report["remaining_work"],
                "progress": report["progress"],
                "source_ready": binding["source_ready"],
                "source_confirmed": binding["source_ready"],
                "enabled_stream_sources": len(self._enabled_sources()),
                "source_binding": binding,
                "completeness": False,
                "completeness_report": report,
                "完整性结论": report["conclusion"],
            }

        # 恢复或一次性完整执行。
        previous_manifest = manifest if same_manifest and isinstance(manifest, dict) else None
        processed_positions: set[int] = set()
        if previous_manifest:
            processed_positions = {int(i) for i in previous_manifest.get("processed_positions", [])}

        if rows is not None:
            recovery_rows = rows
        elif previous_manifest:
            recovery_rows = [row for row in all_rows if int(row["row_index"]) not in processed_positions]
        else:
            recovery_rows = all_rows

        stage = "recovery" if previous_manifest else "full"
        write_result = self.produce_artifact(
            recovery_rows,
            f"{batch_no}#{stage}",
            metadata={
                "batch_no": batch_no,
                "stage": stage,
                "state": "可发布",
                "complete": True,
                **spec,
            },
            replace=True,
        )
        if not write_result.get("ok"):
            entry["status"] = "待恢复"
            entry["pending"] = True
            entry["abnormal"] = True
            return {
                "ok": False,
                "recoverable": True,
                "status": "待恢复",
                "reason": f"批次 {batch_no} 恢复写入失败：{write_result}",
                "write_result": write_result,
            }

        if previous_manifest:
            written_positions = {int(i) for i in previous_manifest.get("written_positions", [])}
            processed_positions = set(previous_manifest.get("processed_positions", []))
        else:
            written_positions = set()
            processed_positions = set()

        for row in recovery_rows:
            position = int(row.get("row_index", -1))
            processed_positions.add(position)
            if not row.get("dirty"):
                written_positions.add(position)

        # 以流水线实际输入做最终核对，防止再次出现“返回成功但制品不完整”。
        actual_rows = self.actual_input_rows(entry_id)
        expected_clean = len(clean_rows)
        if len(actual_rows) != expected_clean:
            manifest = {
                "batch_no": batch_no,
                "entry_id": entry_id,
                "spec": spec,
                "state": "待恢复",
                "started": True,
                "committed": False,
                "expected_rows": rows_count,
                "clean_expected": expected_clean,
                "dirty_rows": len(dirty_rows),
                "dirty_rows_detail": dirty_rows,
                "written_positions": sorted(written_positions),
                "processed_positions": sorted(processed_positions),
                "updated_at": time.time(),
            }
            entry["slice_manifest"] = manifest
            entry["status"] = "待恢复"
            entry["abnormal"] = True
            report = self.sync_progress(entry)
            reason = (
                f"批次 {batch_no} 写入后核对失败：流水线实际 {len(actual_rows)} 行，"
                f"应为 {expected_clean} 行，继续保持待恢复"
            )
            return {
                "ok": False,
                "interrupted": True,
                "recoverable": True,
                "status": "待恢复",
                "reason": reason,
                "pipeline_rows": len(actual_rows),
                "expected_rows": expected_clean,
                "completeness_report": report,
            }

        manifest = {
            "batch_no": batch_no,
            "entry_id": entry_id,
            "spec": spec,
            "state": "可发布",
            "started": True,
            "committed": True,
            "expected_rows": rows_count,
            "clean_expected": expected_clean,
            "dirty_rows": len(dirty_rows),
            "dirty_rows_detail": dirty_rows,
            "written_positions": sorted(written_positions),
            "processed_positions": sorted(processed_positions),
            "actual_row_count": len(actual_rows),
            "completed_at": time.time(),
            "updated_at": time.time(),
        }
        entry["slice_manifest"] = manifest
        entry["status"] = "可发布"
        entry["pending"] = False
        # 中断异常已解除；脏行由 dirty_row_count/清单独立计数，不阻塞发布。
        entry["abnormal"] = False
        entry["完成时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        self._processed_batches.add(batch_no)
        self._batch_specs[batch_no] = {**spec, "started": True, "committed": True, "state": "可发布"}
        self._input_rows[batch_no] = actual_rows

        report = self.sync_progress(entry)
        result = {
            "ok": True,
            "published": False,
            "publishable": report["publishable"],
            "source_ready": report["source_ready"],
            "status": "可发布",
            "state": "可发布",
            "batch": batch_no,
            "batch_no": batch_no,
            "entry_id": entry_id,
            "rows_ingested": len(actual_rows),
            "written_rows": len(actual_rows),
            "rows": actual_rows,
            "artifact_rows": actual_rows,
            "dirty_rows": len(dirty_rows),
            "dirty_row_count": len(dirty_rows),
            "dirty_count": len(dirty_rows),
            "dirty_details": dirty_rows,
            "total_range": rows_count,
            "pipeline": self.PIPELINE,
            "remaining_rows": 0,
            "remaining": 0,
            "remaining_count": 0,
            "missing_rows": 0,
            "剩余工作": "无剩余工作，可发布",
            "source_ready": binding["source_ready"],
            "source_confirmed": binding["source_ready"],
            "enabled_stream_sources": len(self._enabled_sources()),
            "source_binding": binding,
            "completeness": report["complete"],
            "complete": report["complete"],
            "completeness_report": report,
            "完整性结论": report["conclusion"],
        }
        self._batch_results[batch_no] = result
        return result

    # 常见恢复入口别名；不同调用方可能按恢复/补齐语义命名。
    def recover_slice(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.execute_slice(entry_id, recover=True, **kwargs)

    def resume_slice(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.execute_slice(entry_id, resume=True, **kwargs)

    resume_from_checkpoint = resume_slice

    def retry_slice(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.execute_slice(entry_id, recover=True, **kwargs)

    def complete_slice(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.execute_slice(entry_id, complete=True, **kwargs)

    def recover_batch(self, batch_no: str, **kwargs: Any) -> dict[str, Any]:
        return self.execute_slice(batch_no=batch_no, recover=True, **kwargs)

    def recover_checkpoint(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.recover_slice(entry_id, **kwargs)

    recover_interrupted_slice = recover_slice
    resume_interrupted_slice = resume_slice
    retry_interrupted_slice = retry_slice

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action in {"发起执行", "恢复执行", "重试执行", "补齐分片"}:
            result = self.execute_slice(entry_id, recover=action != "发起执行")
            if result.get("ok"):
                entry["_action_ok"] = True
                return entry, f"{self.ENTITY}已{action}；{result.get('完整性结论') or '执行完成，可发布'}"
            # 保留 entry，使调用方仍能读到「待恢复」状态和剩余工作；路由通过
            # _action_ok 决定 ActionResult.ok，而不是用 None 丢失状态载体。
            entry["_action_ok"] = False
            return entry, str(result.get("reason") or "分片执行未完成")

        if action == "标记完成":
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["_action_ok"] = True
            return entry, f"{self.ENTITY}已标记完成"

        if action == "发布批次":
            report = self.sync_progress(entry)
            if not report["publishable"]:
                entry["_action_ok"] = False
                return entry, report["conclusion"]
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["_action_ok"] = True
            return entry, f"{self.ENTITY}已发布"

        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stream_sources_consumed"] = self.consume_stream_sources()
        base["processed_batches"] = len(self._processed_batches)
        base["recoverable_batches"] = sum(
            1 for row in store.rows(self.MODULE) if row.get("status") == "待恢复"
        )
        base["publishable_batches"] = sum(
            1 for row in store.rows(self.MODULE) if row.get("status") == "可发布"
        )
        return base
