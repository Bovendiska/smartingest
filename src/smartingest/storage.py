"""Persistence: bronze files in MinIO, metadata / silver / quarantine in PostgreSQL."""

from __future__ import annotations

import logging
import uuid
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from smartingest.config import Settings
from smartingest.llm import LLMCallRecord
from smartingest.models import ColumnProfile, Issue, IssueStatus, SemanticType
from smartingest.parsing import slugify_column
from smartingest.pipeline import PipelineResult, resolve_row
from smartingest.storage_names import DEMO_TABLE_PREFIX

logger = logging.getLogger(__name__)

PG_TYPES: dict[SemanticType, str] = {
    SemanticType.EMAIL: "text",
    SemanticType.PHONE_ID: "text",
    SemanticType.TEXT: "text",
    SemanticType.DATE: "date",
    SemanticType.CURRENCY_IDR: "bigint",
    SemanticType.INTEGER: "bigint",
    SemanticType.DECIMAL: "numeric",
    SemanticType.BOOLEAN: "boolean",
}


class SchemaDriftError(RuntimeError):
    def __init__(self, changes: list[dict[str, str]]) -> None:
        super().__init__(f"schema drift: {changes}")
        self.changes = changes


def to_python(value: str | None, semantic_type: SemanticType) -> Any:
    """Convert a canonical string into the Python type psycopg should send."""
    if value is None:
        return None
    if semantic_type is SemanticType.DATE:
        return date.fromisoformat(value)
    if semantic_type in (SemanticType.INTEGER, SemanticType.CURRENCY_IDR):
        return int(value)
    if semantic_type is SemanticType.DECIMAL:
        return Decimal(value)
    if semantic_type is SemanticType.BOOLEAN:
        return value == "true"
    return value


class Warehouse:
    def __init__(self, settings: Settings) -> None:
        self._dsn = settings.database_url

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=5)

    # --- demo sessions -------------------------------------------------------------------
    def ping(self) -> None:
        with self.connect() as conn:
            conn.execute("SELECT 1")
    
    def create_demo_session(self, token_hash: str, ip_hash: str, ttl_hours: int) -> dict[str, Any]:
        with self.connect() as conn:
            return conn.execute(
                "INSERT INTO meta.demo_sessions (token_hash, ip_hash, expires_at) "
                "VALUES (%s, %s, now() + make_interval(hours => %s)) RETURNING session_id, expires_at",
                (token_hash, ip_hash, ttl_hours),
            ).fetchone()

    def get_demo_session(self, token_hash: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT session_id, uploads, expires_at FROM meta.demo_sessions "
                "WHERE token_hash = %s AND expires_at > now()",
                (token_hash,),
            ).fetchone()

    def claim_upload(self, session_id: uuid.UUID, max_uploads: int) -> bool:
        """Atomically use one upload from the session's quota."""
        with self.connect() as conn:
            row = conn.execute(
                "UPDATE meta.demo_sessions SET uploads = uploads + 1 "
                "WHERE session_id = %s AND uploads < %s AND expires_at > now() RETURNING uploads",
                (session_id, max_uploads),
            ).fetchone()
            return row is not None

    def release_upload(self, session_id: uuid.UUID) -> None:
        """Give the quota back when an upload was rejected before anything was stored."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE meta.demo_sessions SET uploads = greatest(uploads - 1, 0) WHERE session_id = %s", (session_id,)
            )

    def cleanup_expired_demo(self) -> list[str]:
        """Delete everything that belongs to expired demo sessions. Returns bronze keys to delete."""
        with self.connect() as conn, conn.transaction():
            rows = conn.execute(
                "SELECT b.silver_table, b.object_key FROM meta.batches b "
                "JOIN meta.demo_sessions s ON s.session_id = b.owner_session WHERE s.expires_at <= now()"
            ).fetchall()
            tables = sorted({r["silver_table"] for r in rows})
            for table in tables:
                if not table.startswith(DEMO_TABLE_PREFIX):  # never drop an admin table
                    continue
                conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("silver", table)))
                conn.execute("DELETE FROM meta.schema_registry WHERE source_name = %s", (table,))
            deleted = conn.execute("DELETE FROM meta.demo_sessions WHERE expires_at <= now()").rowcount
            if deleted:
                logger.info("Cleanup: removed %d expired demo sessions and %d tables", deleted, len(tables))
            return [r["object_key"] for r in rows]

    # --- reads ---------------------------------------------------------------------

    def llm_calls_today(self) -> int:
        with self.connect() as conn:
            return conn.execute(
                "SELECT count(*) AS n FROM meta.llm_calls WHERE created_at >= date_trunc('day', now())"
            ).fetchone()["n"]

    def list_batches(self, owner: uuid.UUID | None, is_admin: bool) -> list[dict[str, Any]]:
        query = (
            "SELECT batch_id, source_name, filename, status, rows_total, rows_quarantined, quality_score, created_at "
            "FROM meta.batches"
        )
        with self.connect() as conn:
            if is_admin:
                return conn.execute(query + " ORDER BY created_at DESC LIMIT 200").fetchall()
            return conn.execute(
                query + " WHERE owner_session = %s ORDER BY created_at DESC", (owner,)
            ).fetchall()

    def get_batch(self, batch_id: uuid.UUID) -> dict[str, Any] | None:
        with self.connect() as conn:
            batch = conn.execute(
                "SELECT batch_id, owner_session, source_name, silver_table, filename, status, rows_total, rows_clean, "
                "rows_auto_fixed, rows_quarantined, quality_score, created_at FROM meta.batches WHERE batch_id = %s",
                (batch_id,),
            ).fetchone()
            if batch is None:
                return None
            batch["columns"] = conn.execute(
                "SELECT column_name, original_name, semantic_type, type_confidence, type_source, null_ratio, "
                "match_ratio, required, distinct_count FROM meta.column_profiles WHERE batch_id = %s "
                "ORDER BY column_name",
                (batch_id,),
            ).fetchall()
            return batch

    def list_issues(self, batch_id: uuid.UUID, status: str | None = None) -> list[dict[str, Any]]:
        query = (
            "SELECT issue_id, row_index, column_name, rule, original_value, suggestion, reason, confidence, "
            "source, status, final_value FROM meta.issues WHERE batch_id = %s"
        )
        params: list[Any] = [batch_id]
        if status:
            query += " AND status = %s"
            params.append(status)
        with self.connect() as conn:
            return conn.execute(query + " ORDER BY row_index, column_name LIMIT 5000", params).fetchall()

    def get_issue(self, issue_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT i.issue_id, i.batch_id, b.owner_session, i.row_index, i.column_name, i.rule, i.original_value, "
                "i.suggestion, i.status, p.semantic_type FROM meta.issues i "
                "JOIN meta.batches b ON b.batch_id = i.batch_id "
                "LEFT JOIN meta.column_profiles p ON p.batch_id = i.batch_id AND p.column_name = i.column_name "
                "WHERE i.issue_id = %s",
                (issue_id,),
            ).fetchone()

    # --- writes --------------------------------------------------------------------

    def record_llm_calls(self, batch_id: uuid.UUID | None, calls: list[LLMCallRecord]) -> None:
        if not calls:
            return
        with self.connect() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO meta.llm_calls (batch_id, purpose, model, input_tokens, output_tokens, latency_ms, ok) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [(batch_id, c.purpose, c.model, c.input_tokens, c.output_tokens, c.latency_ms, c.ok) for c in calls],
            )

    def persist(
        self,
        *,
        owner: uuid.UUID | None,
        source: str,
        table: str,
        batch_id: uuid.UUID,
        sha: str,
        filename: str,
        object_key: str,
        result: PipelineResult,
    ) -> str:
        """Write one batch atomically. Returns the batch status. Raises SchemaDriftError."""
        with self.connect() as conn, conn.transaction():
            self._check_schema(conn, table, batch_id, result.profiles)
            self._ensure_silver_table(conn, table, result.profiles)
            status = "needs_review" if result.report.rows_quarantined else "loaded"
            r = result.report
            conn.execute(
                "INSERT INTO meta.batches (batch_id, owner_session, source_name, silver_table, filename, object_key, "
                "file_sha256, status, rows_total, rows_clean, rows_auto_fixed, rows_quarantined, quality_score) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (batch_id, owner, slugify_column(source), table, filename, object_key, sha, status, r.rows_total,
                 r.rows_clean, r.rows_auto_fixed, r.rows_quarantined, r.quality_score),
            )
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO meta.column_profiles (batch_id, column_name, original_name, semantic_type, "
                    "type_confidence, type_source, null_ratio, match_ratio, required, distinct_count) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [(batch_id, p.column_name, p.original_name, p.semantic_type.value, p.type_confidence,
                      p.type_source, p.null_ratio, p.match_ratio, p.required, p.distinct_count)
                     for p in result.profiles],
                )
                cur.executemany(
                    "INSERT INTO meta.issues (batch_id, row_index, column_name, rule, original_value, suggestion, "
                    "reason, confidence, source, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [(batch_id, i.row_index, i.column_name, i.rule, i.original_value, i.suggestion, i.reason,
                      i.confidence.value, i.source, i.status.value) for i in result.issues],
                )
                cur.executemany(
                    "INSERT INTO quarantine.rows (batch_id, row_index, data) VALUES (%s, %s, %s)",
                    [(batch_id, int(idx), Jsonb(row)) for idx, row in result.quarantine.to_dict("index").items()],
                )
            types = {p.column_name: p.semantic_type for p in result.profiles}
            for idx, row in result.clean.to_dict("index").items():
                self._insert_silver(conn, table, batch_id, int(idx), row, types)
        return status

    def resolve_issue(self, issue_id: int, status: IssueStatus, final_value: str | None) -> dict[str, Any]:
        """Record a reviewer decision; move the row out of quarantine when nothing is left open."""
        with self.connect() as conn, conn.transaction():
            issue = conn.execute(
                "UPDATE meta.issues SET status = %s, final_value = %s, resolved_at = now() "
                "WHERE issue_id = %s AND status = 'open' RETURNING batch_id, row_index",
                (status.value, final_value, issue_id),
            ).fetchone()
            if issue is None:
                raise LookupError(f"issue {issue_id} not found or already resolved")
            if status is IssueStatus.ROW_DROPPED:  # dropping a row settles every other issue on it
                conn.execute(
                    "UPDATE meta.issues SET status = 'row_dropped', resolved_at = now() "
                    "WHERE batch_id = %s AND row_index = %s AND status = 'open'",
                    (issue["batch_id"], issue["row_index"]),
                )
            moved = self._try_promote(conn, issue["batch_id"], issue["row_index"])
            if moved:  # keep the batch summary in step with quarantine.rows
                conn.execute(
                    "UPDATE meta.batches SET rows_quarantined = rows_quarantined - 1, "
                    "quality_score = round(100.0 * (rows_total - rows_quarantined + 1) / nullif(rows_total, 0), 2) "
                    "WHERE batch_id = %s AND rows_quarantined > 0",
                    (issue["batch_id"],),
                )
            remaining = conn.execute(
                "SELECT count(*) AS n FROM meta.issues WHERE batch_id = %s AND status = 'open'", (issue["batch_id"],)
            ).fetchone()["n"]
            if remaining == 0:
                conn.execute("UPDATE meta.batches SET status = 'loaded' WHERE batch_id = %s", (issue["batch_id"],))
        return {"issue_id": issue_id, "row_left_quarantine": moved, "open_issues_in_batch": remaining}

    # --- internals -----------------------------------------------------------------

    def _check_schema(
        self, conn: psycopg.Connection, table: str, batch_id: uuid.UUID, profiles: list[ColumnProfile]
    ) -> None:
        known = {
            r["column_name"]: r["semantic_type"]
            for r in conn.execute(
                "SELECT column_name, semantic_type FROM meta.schema_registry WHERE source_name = %s", (table,)
            ).fetchall()
        }
        drift = [
            {"column": p.column_name, "registered": known[p.column_name], "incoming": p.semantic_type.value}
            for p in profiles
            if p.column_name in known and known[p.column_name] != p.semantic_type.value
        ]
        if drift:
            raise SchemaDriftError(drift)
        new = [p for p in profiles if p.column_name not in known]
        if new:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO meta.schema_registry (source_name, column_name, semantic_type, first_batch_id) "
                    "VALUES (%s, %s, %s, %s)",
                    [(table, p.column_name, p.semantic_type.value, batch_id) for p in new],
                )

    def _ensure_silver_table(self, conn: psycopg.Connection, table: str, profiles: list[ColumnProfile]) -> None:
        ident = sql.Identifier("silver", table)
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (_batch_id uuid NOT NULL, _row_index integer NOT NULL, "
                "_loaded_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (_batch_id, _row_index))"
            ).format(ident)
        )
        for p in profiles:  # new columns from later batches are added, never dropped
            conn.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    ident, sql.Identifier(p.column_name), sql.SQL(PG_TYPES[p.semantic_type])
                )
            )

    def _insert_silver(
        self,
        conn: psycopg.Connection,
        table: str,
        batch_id: uuid.UUID,
        row_index: int,
        row: dict[str, str | None],
        types: dict[str, SemanticType],
    ) -> None:
        columns = list(row)
        query = sql.SQL("INSERT INTO {} (_batch_id, _row_index, {}) VALUES (%s, %s, {}) ON CONFLICT DO NOTHING").format(
            sql.Identifier("silver", table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        conn.execute(query, [batch_id, row_index, *(to_python(row[c], types[c]) for c in columns)])

    def _try_promote(self, conn: psycopg.Connection, batch_id: uuid.UUID, row_index: int) -> bool:
        quarantined = conn.execute(
            "SELECT data FROM quarantine.rows WHERE batch_id = %s AND row_index = %s", (batch_id, row_index)
        ).fetchone()
        if quarantined is None:
            return False
        issues = [
            Issue(**r)
            for r in conn.execute(
                "SELECT row_index, column_name, rule, original_value, suggestion, reason, confidence, source, status, "
                "final_value FROM meta.issues WHERE batch_id = %s AND row_index = %s",
                (batch_id, row_index),
            ).fetchall()
        ]
        if any(i.status is IssueStatus.OPEN for i in issues):
            return False
        final = resolve_row(quarantined["data"], issues)
        if final is not None:
            table = conn.execute(
                "SELECT silver_table FROM meta.batches WHERE batch_id = %s", (batch_id,)
            ).fetchone()["silver_table"]
            types = {
                r["column_name"]: SemanticType(r["semantic_type"])
                for r in conn.execute(
                    "SELECT column_name, semantic_type FROM meta.column_profiles WHERE batch_id = %s", (batch_id,)
                ).fetchall()
            }
            self._insert_silver(conn, table, batch_id, row_index, final, types)
        conn.execute("DELETE FROM quarantine.rows WHERE batch_id = %s AND row_index = %s", (batch_id, row_index))
        return True
