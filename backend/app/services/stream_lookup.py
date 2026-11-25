"""stream_lookup 领域服务：流式关联与维表Join。

业务逻辑：
1. 流式关联：左流来源（stream）+ 右表来源（维表）按关联键关联
2. 关联链路：stream_lookup → merge_plan（关联结果用于流批合并）
3. 失败场景：窗口超时无匹配、缓存击穿、未匹配数据丢失
4. 与上下游流水线：输出关联结果到 merge_plan

跨模块数据流：
  stream_lookup → merge_plan（关联结果参与流批合并决策）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamLookupService(BaseModuleService):
    MODULE = "stream_lookup"
    ENTITY = "关联规则"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["规则编号", "左流来源", "右表来源", "关联键列", "关联类型", "窗口时长", "缓存策略", "未匹配处理"]
    REQUIRED_FIELDS = ["规则编号", "左流来源", "右表来源"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    PIPELINE = "stream_lookup_join"
    DOWNSTREAM_MODULES = ["merge_plan"]
    PRODUCES_ARTIFACT = "流式关联结果记录"

    def execute_lookup(self, entry_id: int, left_events: list[dict[str, Any]]) -> dict[str, Any]:
        """执行流式关联：左流数据与右表维表Join。

        失败场景：
        - 窗口时长超限 → 未匹配
        - 缓存策略不匹配 → 缓存击穿
        - 维表不存在 → 关联失败
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"规则 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"规则未生效，当前状态: {entry.get('status')}"}

        join_key = str(entry.get("关联键列", "id"))
        join_type = str(entry.get("关联类型", "inner"))
        window_ms = float(str(entry.get("窗口时长", "60000")))
        unmatched_policy = str(entry.get("未匹配处理", "drop"))

        # 模拟右表维表数据
        right_table = [
            {"id": 1, "dim_value": "dim_a"},
            {"id": 2, "dim_value": "dim_b"},
            {"id": 3, "dim_value": "dim_c"},
        ]
        right_map = {r["id"]: r for r in right_table}

        matched = []
        unmatched = []
        now = time.time()

        for event in left_events:
            event_ts = event.get("ts", now)
            if abs(now - event_ts) * 1000 > window_ms:
                unmatched.append({"event": event, "reason": f"窗口超时({window_ms}ms)"})
                continue
            key = event.get(join_key)
            right_row = right_map.get(key)
            if right_row:
                matched.append({
                    **event,
                    "dim_value": right_row["dim_value"],
                    "source_identifier": entry.get("左流来源"),
                    "lookup_rule": entry.get("规则编号"),
                })
            else:
                if unmatched_policy == "drop":
                    unmatched.append({"event": event, "reason": "无匹配维度，已丢弃"})
                elif unmatched_policy == "keep":
                    matched.append({
                        **event,
                        "dim_value": None,
                        "source_identifier": entry.get("左流来源"),
                        "lookup_rule": entry.get("规则编号"),
                    })

        batch_id = f"LOOKUP_{entry_id}_{int(time.time())}"
        self.produce_artifact(
            matched,
            batch_id,
            metadata={
                "source_identifier": entry.get("左流来源"),
                "lookup_rule": entry.get("规则编号"),
                "join_key": join_key,
            },
        )

        return {
            "ok": True,
            "entry_id": entry_id,
            "join_key": join_key,
            "join_type": join_type,
            "matched": len(matched),
            "unmatched": len(unmatched),
            "unmatched_samples": unmatched[:3],
        }

    def emit_join_results_for_merge(self) -> list[dict[str, Any]]:
        """向 merge_plan 暴露关联结果"""
        pl = store.pipeline(self.PIPELINE)
        if pl and pl._artifacts:
            return pl._artifacts[:20]
        return []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_rules"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        return base