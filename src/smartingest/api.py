"""FastAPI service: upload a file, review issues, resolve them.

Access model
- Admin (the owner): header ``X-API-Key: <ADMIN_API_KEY>``. Full access, bigger limits.
- Demo visitor: ``POST /demo/sessions`` returns a token; send it as ``X-Demo-Token``.
  A visitor sees only their own batches, has a small quota, and everything they
  upload is deleted when the session expires (24 hours by default).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any

from fastapi import Depends, FastAPI, File, Header, HTTPException, Path, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from smartingest.config import Settings, get_settings
from smartingest.llm import Advisor, LLMCallRecord, NullAdvisor, build_advisor
from smartingest.models import IssueStatus, SemanticType
from smartingest.parsing import read_table
from smartingest.pipeline import run_pipeline
from smartingest.security import (
    RateLimiter,
    UploadRejected,
    check_file_content,
    check_table_shape,
    hash_token,
    new_token,
    safe_extension,
    same_secret,
)
from smartingest.bronze import BronzeStore
from smartingest.storage import SchemaDriftError, Warehouse
from smartingest.storage_names import DEMO_TABLE_PREFIX, batch_id_for, silver_table_for
from smartingest.validators import ColumnContext, check_value

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("smartingest.api")

SOURCE_PATTERN = r"^[A-Za-z0-9_-]{1,40}$"
CLEANUP_INTERVAL_SECONDS = 15 * 60
DEMO_NOTICE = (
    "Public demo: upload dummy data only, never real personal data. "
    "Everything you upload is deleted automatically when your session expires."
)


async def _cleanup_loop() -> None:
    while True:
        try:
            settings = get_settings()
            keys = await asyncio.to_thread(Warehouse(settings).cleanup_expired_demo)
            await asyncio.to_thread(BronzeStore(settings).delete, keys)
        except Exception:
            logger.exception("Demo cleanup failed; will retry")
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    get_settings()  # fail fast on unsafe configuration
    task = asyncio.create_task(_cleanup_loop())
    yield
    task.cancel()


app = FastAPI(
    title="SmartIngest",
    version="0.2.0",
    description="Upload messy files, get clean tables.\n\n" + DEMO_NOTICE,
    lifespan=lifespan,
)

_request_limiter: RateLimiter | None = None
_session_limiter: RateLimiter | None = None


def _limiters(settings: Settings) -> tuple[RateLimiter, RateLimiter]:
    global _request_limiter, _session_limiter
    if _request_limiter is None or _session_limiter is None:
        _request_limiter = RateLimiter(settings.requests_per_minute_per_ip, 60)
        _session_limiter = RateLimiter(settings.demo_sessions_per_ip_per_hour, 3600)
    return _request_limiter, _session_limiter


def client_ip(request: Request) -> str:
    # Uvicorn runs with --proxy-headers behind Caddy, so this is the visitor's address.
    return request.client.host if request.client else "unknown"


@app.middleware("http")
async def rate_limit(request: Request, call_next):  # type: ignore[no-untyped-def]
    if request.url.path != "/health":
        limiter, _ = _limiters(get_settings())
        if not limiter.allow(client_ip(request)):
            return JSONResponse({"detail": "Too many requests, slow down"}, status_code=429)
    return await call_next(request)


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    # Details go to the server log only; the visitor gets an id to report.
    error_id = uuid.uuid4().hex[:12]
    logger.exception("Unhandled error %s on %s %s", error_id, request.method, request.url.path)
    return JSONResponse({"detail": "Internal error", "error_id": error_id}, status_code=500)


# --- Dependencies -------------------------------------------------------------------------


def warehouse(settings: Annotated[Settings, Depends(get_settings)]) -> Warehouse:
    return Warehouse(settings)


def bronze(settings: Annotated[Settings, Depends(get_settings)]) -> BronzeStore:
    return BronzeStore(settings)


@dataclass(frozen=True)
class Principal:
    is_admin: bool
    session_id: uuid.UUID | None


def principal(
    settings: Annotated[Settings, Depends(get_settings)],
    db: Annotated[Warehouse, Depends(warehouse)],
    x_api_key: Annotated[str | None, Header()] = None,
    x_demo_token: Annotated[str | None, Header()] = None,
) -> Principal:
    if x_api_key is not None:
        if same_secret(x_api_key, settings.admin_api_key):
            return Principal(is_admin=True, session_id=None)
        raise HTTPException(401, "Invalid API key")
    if x_demo_token and settings.demo_mode:
        session = db.get_demo_session(hash_token(x_demo_token))
        if session is not None:
            return Principal(is_admin=False, session_id=session["session_id"])
        raise HTTPException(401, "Demo session expired or invalid; create a new one at POST /demo/sessions")
    raise HTTPException(401, "Send X-Demo-Token (from POST /demo/sessions) or X-API-Key")


def assert_owns(who: Principal, owner_session: uuid.UUID | None) -> None:
    # Respond 404, not 403, so visitors cannot probe which batch ids exist.
    if not who.is_admin and owner_session != who.session_id:
        raise HTTPException(404, "Not found")


# --- Routes ---------------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/demo/sessions", status_code=201)
def create_demo_session(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    db: Annotated[Warehouse, Depends(warehouse)],
) -> dict[str, Any]:
    if not settings.demo_mode:
        raise HTTPException(404, "Demo mode is disabled")
    ip = client_ip(request)
    _, session_limiter = _limiters(settings)
    if not session_limiter.allow(ip):
        raise HTTPException(429, "Too many demo sessions from your network; try again later")
    token = new_token()
    session = db.create_demo_session(
        hash_token(token), hashlib.sha256(ip.encode()).hexdigest(), settings.demo_session_ttl_hours
    )
    limits = settings.demo_limits
    return {
        "token": token,  # shown once; only its hash is stored
        "expires_at": session["expires_at"],
        "notice": DEMO_NOTICE,
        "limits": {
            "uploads": settings.demo_max_uploads_per_session,
            "max_file_mb": limits.max_bytes // (1024 * 1024),
            "max_rows": limits.max_rows,
            "max_columns": limits.max_columns,
        },
    }


def _advisor_for(who: Principal, settings: Settings, db: Warehouse, calls: list[LLMCallRecord]) -> Advisor:
    if not who.is_admin and not settings.demo_llm_enabled:
        return NullAdvisor()
    if db.llm_calls_today() >= settings.llm_daily_call_limit:
        logger.warning("Daily LLM call limit reached; running rule-only")
        return NullAdvisor()
    return build_advisor(
        on_call=calls.append,
        max_items=settings.llm_items_per_upload,
        allow_raw_values=who.is_admin,  # visitors' files may contain real personal data: always mask
    )


@app.post("/sources/{source}/uploads", status_code=201)
def upload(
    source: Annotated[str, Path(pattern=SOURCE_PATTERN)],
    file: Annotated[UploadFile, File()],
    who: Annotated[Principal, Depends(principal)],
    settings: Annotated[Settings, Depends(get_settings)],
    db: Annotated[Warehouse, Depends(warehouse)],
    store: Annotated[BronzeStore, Depends(bronze)],
) -> dict[str, Any]:
    limits = settings.admin_limits if who.is_admin else settings.demo_limits
    if source.lower().startswith(DEMO_TABLE_PREFIX):
        raise HTTPException(422, f"Source names may not start with '{DEMO_TABLE_PREFIX}'")
    try:
        ext = safe_extension(file.filename)
        content = file.file.read(limits.max_bytes + 1)  # never read more than the limit
        check_file_content(content, ext, limits)
    except UploadRejected as exc:
        raise HTTPException(422, str(exc)) from exc

    batch_id, sha = batch_id_for(who.session_id, source, content)
    existing = db.get_batch(batch_id)
    if existing is not None:  # idempotent: same file, same source, same owner = same batch
        return {"batch_id": str(batch_id), "already_ingested": True, "status": existing["status"]}

    if not who.is_admin and not db.claim_upload(who.session_id, settings.demo_max_uploads_per_session):
        raise HTTPException(429, "Upload quota for this demo session is used up")

    try:
        try:
            df = read_table(content, f"upload{ext}")
        except Exception as exc:  # malformed CSV / Excel / JSON: details stay in the log
            logger.info("Parse failure for batch %s: %s", batch_id, exc)
            raise UploadRejected("Could not read the file; check that it is a valid CSV, XLSX or JSON") from exc
        longest = max((len(v) for col in df.columns for v in df[col] if v is not None), default=0)
        check_table_shape(len(df), len(df.columns), longest, limits)
    except UploadRejected as exc:
        if not who.is_admin:
            db.release_upload(who.session_id)
        raise HTTPException(422, str(exc)) from exc

    calls: list[LLMCallRecord] = []
    advisor = _advisor_for(who, settings, db, calls)
    result = run_pipeline(df, advisor=advisor, auto_apply_high=settings.auto_apply_high)

    table = silver_table_for(who.session_id, source)
    object_key = store.put(table, batch_id, ext, content)
    try:
        status = db.persist(
            owner=who.session_id, source=source, table=table, batch_id=batch_id, sha=sha,
            filename=f"upload{ext}", object_key=object_key, result=result,
        )
    except SchemaDriftError as exc:
        store.delete([object_key])
        db.record_llm_calls(None, calls)
        raise HTTPException(409, {"message": "Schema drift detected; batch not loaded", "changes": exc.changes}) from exc
    except Exception:
        store.delete([object_key])  # the database transaction rolled back; do not leave an orphan file
        raise
    db.record_llm_calls(batch_id, calls)

    return {
        "batch_id": str(batch_id),
        "status": status,
        "report": result.report.model_dump(),
        "columns": [
            {"name": p.column_name, "original": p.original_name, "type": p.semantic_type.value,
             "confidence": p.type_confidence, "source": p.type_source}
            for p in result.profiles
        ],
    }


@app.get("/batches")
def list_batches(
    who: Annotated[Principal, Depends(principal)], db: Annotated[Warehouse, Depends(warehouse)]
) -> list[dict[str, Any]]:
    return db.list_batches(who.session_id, who.is_admin)


@app.get("/batches/{batch_id}")
def get_batch(
    batch_id: uuid.UUID,
    who: Annotated[Principal, Depends(principal)],
    db: Annotated[Warehouse, Depends(warehouse)],
) -> dict[str, Any]:
    batch = db.get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, "Not found")
    assert_owns(who, batch["owner_session"])
    return batch


@app.get("/batches/{batch_id}/issues")
def get_issues(
    batch_id: uuid.UUID,
    who: Annotated[Principal, Depends(principal)],
    db: Annotated[Warehouse, Depends(warehouse)],
    status: Annotated[IssueStatus | None, Query()] = IssueStatus.OPEN,
) -> list[dict[str, Any]]:
    batch = db.get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, "Not found")
    assert_owns(who, batch["owner_session"])
    return db.list_issues(batch_id, status.value if status else None)


class Action(str, Enum):
    ACCEPT = "accept"          # take the suggestion
    EDIT = "edit"              # use the reviewer's own value
    DROP_VALUE = "drop_value"  # store NULL for this cell
    DROP_ROW = "drop_row"      # discard the whole row


class Resolution(BaseModel):
    action: Action
    value: str | None = Field(default=None, max_length=2_000)


@app.post("/issues/{issue_id}/resolve")
def resolve(
    issue_id: int,
    body: Resolution,
    who: Annotated[Principal, Depends(principal)],
    db: Annotated[Warehouse, Depends(warehouse)],
) -> dict[str, Any]:
    issue = db.get_issue(issue_id)
    if issue is None:
        raise HTTPException(404, "Not found")
    assert_owns(who, issue["owner_session"])
    if issue["status"] != IssueStatus.OPEN.value:
        raise HTTPException(409, f"Issue already {issue['status']}")

    final_value: str | None = None
    if body.action is Action.ACCEPT:
        if issue["suggestion"] is None or issue["suggestion"] == "drop_row":
            raise HTTPException(422, "This issue has no value suggestion; use edit, drop_value or drop_row")
        status = IssueStatus.ACCEPTED
    elif body.action is Action.EDIT:
        if body.value is None:
            raise HTTPException(422, "edit needs a value")
        if issue["semantic_type"] is not None:
            finding = check_value(body.value, SemanticType(issue["semantic_type"]), ColumnContext())
            if finding is not None:  # the reviewer's value must itself be canonical
                raise HTTPException(422, {"message": "Value is not valid for this column", "rule": finding.rule,
                                          "suggestion": finding.suggestion})
        status, final_value = IssueStatus.EDITED, body.value
    elif body.action is Action.DROP_VALUE:
        status = IssueStatus.VALUE_DROPPED
    else:
        status = IssueStatus.ROW_DROPPED

    try:
        return db.resolve_issue(issue_id, status, final_value)
    except LookupError as exc:
        raise HTTPException(409, "Issue was already resolved") from exc
