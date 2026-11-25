"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def domain_response(result: dict[str, Any]) -> dict[str, Any] | JSONResponse:
    """领域富结果默认沿用 200+ok 契约；明确标记冲突时返回对应 HTTP 状态。"""
    status_code = result.get("status_code")
    if result.get("conflict") and isinstance(status_code, int):
        return JSONResponse(status_code=status_code, content=result)
    return result


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/batch_slice/progress")
def batch_slice_progress() -> list[dict[str, Any]]:
    service = require_service("batch_slice")
    return service.slice_progress()


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    action_ok = entry is not None
    if entry is not None and "_action_ok" in entry:
        action_ok = bool(entry.pop("_action_ok"))
    return ActionResult(ok=action_ok, message=message, entry=entry)


# 领域富结果入口：通用 actions 契约只返回 ok/message/entry；这些路径保留
# execute_slice/execute_merge 的完整进度、冲突和实际输入核对结果。
@router.post("/batch_slice/submit")
def submit_batch_slice(payload: dict[str, Any]) -> Any:
    service = require_service("batch_slice")
    values = payload.get("values", payload) if isinstance(payload, dict) else {}
    execute = bool(payload.get("execute", False)) if isinstance(payload, dict) else False
    return domain_response(service.submit_slice(values, execute=execute))


@router.post("/batch_slice/{entry_id}/execute")
@router.post("/batch_slice/{entry_id}/execute_slice")
def execute_batch_slice(entry_id: int, payload: dict[str, Any] | None = None):
    service = require_service("batch_slice")
    options = payload or {}
    return domain_response(service.execute_slice(entry_id, **options))


@router.post("/batch_slice/{entry_id}/recover")
@router.post("/batch_slice/{entry_id}/recover_slice")
@router.post("/batch_slice/{entry_id}/resume")
def recover_batch_slice(entry_id: int, payload: dict[str, Any] | None = None):
    service = require_service("batch_slice")
    options = payload or {}
    return domain_response(service.recover_slice(entry_id, **options))


@router.get("/batch_slice/{entry_id}/completeness")
def batch_completeness(entry_id: int) -> dict[str, Any]:
    service = require_service("batch_slice")
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分片批次 {entry_id} 不存在或已归档")
    return service.sync_progress(entry)


@router.post("/merge_plan/{entry_id}/execute")
@router.post("/merge_plan/{entry_id}/execute_merge")
@router.post("/merge_plan/{entry_id}/merge")
def execute_merge_plan(entry_id: int, payload: dict[str, Any] | None = None):
    service = require_service("merge_plan")
    options = payload or {}
    return domain_response(service.execute_merge(entry_id, **options))


@router.post("/merge_plan/{entry_id}/publish")
@router.post("/merge_plan/{entry_id}/publish_merge")
def publish_merge_plan(entry_id: int, payload: dict[str, Any] | None = None):
    service = require_service("merge_plan")
    options = payload or {}
    return domain_response(service.publish_merge(entry_id, **options))


@router.get("/merge_plan/{entry_id}/readiness")
def merge_readiness(entry_id: int) -> dict[str, Any]:
    service = require_service("merge_plan")
    return service.check_readiness(entry_id)
