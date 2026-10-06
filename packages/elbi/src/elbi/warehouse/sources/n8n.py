"""n8n connector: workflow and execution facts, read straight from n8n's own database.

n8n's public REST API pages executions through the same process that runs workflows, so
reading a large execution history that way competes with the workflows themselves. This
connector reads the database underneath instead, read-only: SQLite opened with
``mode=ro`` (the default install), or Postgres through a read-only role.

Three tables:

* ``workflows``: one row per workflow, with whether an error workflow is set, when the
  running version was published, and the outcome facts a health monitor needs
  (latest real success and error, ``broken_now``, ``awaiting_proof``).
* ``executions``: one row per execution started inside the history window. Status
  changes after insert (``running`` becomes ``success``), so this is full-refresh over
  the window rather than incremental on ``id``.
* ``run_errors``: what failed, decoded from a capped sample of execution payloads. An
  errored run yields its failing node and message; a successful run whose node error was
  swallowed by continue-on-fail yields a ``swallowed`` row, which no status count shows.

"Real" means not ``manual``: a test run from the editor, often on pinned data, proves
nothing about the live trigger path, so it neither clears nor raises a breakage.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

from ..config import SourceConfig, SourceField, SourceSchema
from .base import SimpleSource, SourceInputs
from .registry import SourceRegistry

TABLES = ("workflows", "executions", "run_errors")

_FAILED = ("error", "crashed")
_DEFAULT_HISTORY_DAYS = 7
_DEFAULT_SAMPLE_PER_WORKFLOW = 8
# A payload over this many characters is skipped: decoding costs memory in proportion
# to size, and a run that large is rarely the one that swallowed an error.
_DEFAULT_SAMPLE_MAX_CHARS = 400_000
_MESSAGE_CAP = 500
_PREFIX = re.compile(r"[A-Za-z0-9_]*")

_WORKFLOW_COLUMNS: dict[str, pa.DataType] = {
    "id": pa.string(),
    "name": pa.string(),
    "active": pa.bool_(),
    "archived": pa.bool_(),
    "error_workflow": pa.string(),
    "has_error_workflow": pa.bool_(),
    "save_success": pa.string(),
    "trigger_types": pa.string(),
    "created_at": pa.timestamp("us", tz="UTC"),
    "updated_at": pa.timestamp("us", tz="UTC"),
    "published_at": pa.timestamp("us", tz="UTC"),
    "last_auto_success_at": pa.timestamp("us", tz="UTC"),
    "last_auto_error_at": pa.timestamp("us", tz="UTC"),
    "last_manual_success_at": pa.timestamp("us", tz="UTC"),
    "auto_errors_window": pa.int64(),
    "broken_now": pa.bool_(),
    "awaiting_proof": pa.bool_(),
}
_EXECUTION_COLUMNS: dict[str, pa.DataType] = {
    "id": pa.string(),
    "workflow_id": pa.string(),
    "status": pa.string(),
    "mode": pa.string(),
    "started_at": pa.timestamp("us", tz="UTC"),
    "stopped_at": pa.timestamp("us", tz="UTC"),
}
_RUN_ERROR_COLUMNS: dict[str, pa.DataType] = {
    "execution_id": pa.string(),
    "workflow_id": pa.string(),
    "kind": pa.string(),
    "status": pa.string(),
    "started_at": pa.timestamp("us", tz="UTC"),
    "node": pa.string(),
    "message": pa.string(),
}


def revive(flat: list[Any]) -> Any:
    """Decode n8n's ``flatted`` serialization back into the object it encodes.

    n8n stores execution data as a flat JSON array: entry 0 is the root, and every
    string inside an object or array is the index of another entry. Shared and circular
    references come back as the same object, which is why the memo is filled before an
    entry's children are visited.
    """
    memo: dict[int, Any] = {}

    def get(index: int) -> Any:
        if index in memo:
            return memo[index]
        value = flat[index]
        if isinstance(value, dict):
            obj: dict[str, Any] = {}
            memo[index] = obj
            for key, item in value.items():
                obj[key] = get(int(item)) if isinstance(item, str) else item
            return obj
        if isinstance(value, list):
            arr: list[Any] = []
            memo[index] = arr
            for item in value:
                arr.append(get(int(item)) if isinstance(item, str) else item)
            return arr
        memo[index] = value
        return value

    return get(0) if flat else None


def failure_of(data: dict[str, Any]) -> tuple[str | None, str | None]:
    """The failing node and message of an errored run's decoded execution data."""
    result = data.get("resultData") or {}
    error = result.get("error") or {}
    node = error.get("node")
    name = node.get("name") if isinstance(node, dict) else None
    return name or result.get("lastNodeExecuted"), error.get("message")


def swallowed_by(data: dict[str, Any]) -> tuple[str, str | None] | None:
    """The first node error a successful run swallowed, as ``(node, message)``.

    Continue-on-fail turns a node error into a normal output: either the node run keeps
    an ``error`` beside its output, or the output item is the bare error object (a
    ``json`` holding ``error`` and little else). Either way the run reports success.
    """
    run_data = (data.get("resultData") or {}).get("runData") or {}
    for node, runs in run_data.items():
        for run in runs or []:
            if not run:
                continue
            error = run.get("error")
            if error:
                message = error.get("message") if isinstance(error, dict) else None
                return node, message if message is None else str(message)
            for output in (run.get("data") or {}).get("main") or []:
                for item in output or []:
                    body = (item or {}).get("json") or {}
                    if isinstance(body, dict) and body.get("error") and len(body) <= 3:
                        inner = body["error"]
                        text = (
                            inner.get("message") if isinstance(inner, dict) else inner
                        )
                        return node, None if text is None else str(text)
    return None


def _utc(value: Any) -> datetime | None:
    """A database timestamp as an aware UTC datetime.

    SQLite returns n8n's timestamps as ``YYYY-MM-DD HH:MM:SS.fff`` text in UTC; Postgres
    returns datetimes, naive or aware depending on the column type.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _later(a: datetime | None, b: datetime | None) -> bool:
    """Whether ``a`` is set and after ``b`` (an unset ``b`` is the far past)."""
    return a is not None and (b is None or a > b)


def _table(rows: list[dict[str, Any]], columns: dict[str, pa.DataType]) -> pa.Table:
    """Rows as Arrow with a fixed schema, so an empty batch keeps its columns."""
    return pa.Table.from_pylist(rows, schema=pa.schema(list(columns.items())))


@SourceRegistry.register
class N8nSource(SimpleSource):
    """Read n8n workflows, executions and run errors from n8n's own database."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        """Hold the clock the history window is measured from (tests pin it)."""
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def source_type(self) -> str:
        """The registry key for this connector."""
        return "n8n"

    @property
    def config(self) -> SourceConfig:
        """The catalog entry and connection form."""
        return SourceConfig(
            name="n8n",
            label="n8n",
            category="Engineering & monitoring",
            icon="n8n",
            release_status="beta",
            caption="Workflow health from n8n's own database, read-only: which "
            "workflows fail, which fail quietly, and which fixes no real run has "
            "proven yet.",
            fields=[
                SourceField(
                    name="backend",
                    label="Database",
                    type="select",
                    default="sqlite",
                    options=[
                        {"value": "sqlite", "label": "SQLite (default install)"},
                        {"value": "postgres", "label": "Postgres"},
                    ],
                ),
                SourceField(
                    name="database",
                    label="SQLite file",
                    required=False,
                    placeholder="/home/node/.n8n/database.sqlite",
                    caption="Opened read-only; n8n can keep running.",
                    depends_on="backend",
                    depends_value="sqlite",
                ),
                SourceField(
                    name="url",
                    label="Postgres URL",
                    type="password",
                    required=False,
                    placeholder="postgresql://readonly:...@host:5432/n8n",
                    caption="Use a role with SELECT only.",
                    depends_on="backend",
                    depends_value="postgres",
                ),
                SourceField(
                    name="table_prefix",
                    label="Table prefix",
                    required=False,
                    placeholder="(none)",
                    caption="n8n's DB_TABLE_PREFIX, if one is set.",
                ),
                SourceField(
                    name="history_days",
                    label="History window (days)",
                    type="number",
                    required=False,
                    default=_DEFAULT_HISTORY_DAYS,
                ),
                SourceField(
                    name="sample_per_workflow",
                    label="Runs decoded per workflow",
                    type="number",
                    required=False,
                    default=_DEFAULT_SAMPLE_PER_WORKFLOW,
                    caption="Recent runs whose payload is read for errors.",
                ),
            ],
        )

    def validate(self, config: dict[str, Any]) -> tuple[bool, list[str]]:
        """Require the field the chosen backend needs, and only that one."""
        backend = config.get("backend") or "sqlite"
        if backend not in ("sqlite", "postgres"):
            return False, [f"Unknown backend: {backend}"]
        needed = "database" if backend == "sqlite" else "url"
        if not config.get(needed):
            return False, [f"Missing required field: {needed}"]
        if not _PREFIX.fullmatch(str(config.get("table_prefix") or "")):
            return False, ["Table prefix may hold only letters, digits and _"]
        return True, []

    def schemas(self, config: dict[str, Any]) -> list[SourceSchema]:
        """The three tables, all full-refresh (status changes after insert)."""
        return [SourceSchema(name=name) for name in TABLES]

    def extract(self, inputs: SourceInputs) -> Iterator[pa.Table]:
        """Read one table; the connection is opened read-only and closed after."""
        if inputs.schema not in TABLES:
            raise ValueError(f"Unknown n8n table: {inputs.schema}")
        reader = _Reader(inputs.config, self._clock())
        with reader.engine().connect() as conn:
            if inputs.schema == "workflows":
                yield _table(reader.workflows(conn), _WORKFLOW_COLUMNS)
            elif inputs.schema == "executions":
                yield _table(reader.executions(conn), _EXECUTION_COLUMNS)
            else:
                yield _table(reader.run_errors(conn), _RUN_ERROR_COLUMNS)


class _Reader:
    """The queries, written once for both SQLite and Postgres.

    Identifiers are double-quoted (n8n's columns are camelCase, which Postgres folds
    without quotes) and time arithmetic happens here rather than in SQL, so no dialect
    function is needed. The only text spliced in is the validated table prefix; every
    value is a bound parameter, hence the ``S608`` waivers.
    """

    def __init__(self, config: dict[str, Any], now: datetime) -> None:
        self.config = config
        self.backend = config.get("backend") or "sqlite"
        self.prefix = str(config.get("table_prefix") or "")
        # The prefix is spliced into table names, so it is held to what n8n allows.
        if not _PREFIX.fullmatch(self.prefix):
            raise ValueError(f"Invalid n8n table prefix: {self.prefix!r}")
        self.now = now
        days = int(config.get("history_days") or _DEFAULT_HISTORY_DAYS)
        self.cutoff = now - timedelta(days=days)
        self.per_workflow = int(
            config.get("sample_per_workflow") or _DEFAULT_SAMPLE_PER_WORKFLOW
        )
        self.max_chars = int(
            config.get("sample_max_chars") or _DEFAULT_SAMPLE_MAX_CHARS
        )

    def engine(self) -> Engine:
        """A read-only engine for the configured backend."""
        from sqlalchemy import create_engine

        if self.backend == "sqlite":
            path = str(self.config["database"])
            return create_engine(f"sqlite:///file:{path}?mode=ro&uri=true")
        url = str(self.config["url"])
        for scheme in ("postgres://", "postgresql://"):
            if url.startswith(scheme):
                url = "postgresql+psycopg://" + url[len(scheme) :]
        return create_engine(url)

    def t(self, name: str) -> str:
        """A quoted table name with n8n's prefix."""
        return f'"{self.prefix}{name}"'

    def bound_cutoff(self) -> Any:
        """The window start in the form the backend compares against.

        SQLite holds n8n's timestamps as text, so the bound is text in the same layout
        (which sorts as time); Postgres compares real timestamps.
        """
        if self.backend == "sqlite":
            return self.cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return self.cutoff

    def columns(self, conn: Connection, table: str) -> set[str]:
        """The column names of one n8n table (schemas vary across n8n versions)."""
        from sqlalchemy import inspect

        return {c["name"] for c in inspect(conn).get_columns(self.prefix + table)}

    def workflows(self, conn: Connection) -> list[dict[str, Any]]:
        """One row per workflow, with its outcome facts folded in."""
        from sqlalchemy import text

        cols = self.columns(conn, "workflow_entity")
        archived = 'w."isArchived"' if "isArchived" in cols else "NULL"
        published = "NULL"
        join = ""
        if "activeVersionId" in cols:
            published = 'h."createdAt"'
            join = (
                f" LEFT JOIN {self.t('workflow_history')} h"
                ' ON h."versionId" = w."activeVersionId"'
            )
        sql = (
            'SELECT w."id", w."name", w."active", '  # noqa: S608
            f"{archived} AS archived, "
            'w."settings", w."nodes", w."createdAt", w."updatedAt", '
            f"{published} AS published_at FROM {self.t('workflow_entity')} w{join}"
        )
        stats = self.outcomes(conn)
        out = []
        for row in conn.execute(text(sql)).mappings():
            settings = _json(row["settings"], {})
            nodes = _json(row["nodes"], [])
            triggers = sorted(
                {
                    str(n.get("type"))
                    for n in nodes
                    if isinstance(n, dict)
                    and not n.get("disabled")
                    and _is_trigger(str(n.get("type") or ""))
                }
            )
            wid = str(row["id"])
            st = stats.get(wid, {})
            last_ok = st.get("last_auto_success_at")
            last_err = st.get("last_auto_error_at")
            published_at = _utc(row["published_at"])
            # Broken: the latest real run that finished failed. Recent: inside the
            # window, so a workflow that failed once a month ago and was never run
            # again does not page anyone today.
            broken = _later(last_err, last_ok)
            error_workflow = settings.get("errorWorkflow") or None
            out.append(
                {
                    "id": wid,
                    "name": row["name"],
                    "active": bool(row["active"]),
                    "archived": bool(row["archived"]),
                    "error_workflow": error_workflow,
                    "has_error_workflow": error_workflow is not None,
                    "save_success": settings.get("saveDataSuccessExecution"),
                    "trigger_types": ",".join(triggers),
                    "created_at": _utc(row["createdAt"]),
                    "updated_at": _utc(row["updatedAt"]),
                    "published_at": published_at,
                    "last_auto_success_at": last_ok,
                    "last_auto_error_at": last_err,
                    "last_manual_success_at": st.get("last_manual_success_at"),
                    "auto_errors_window": st.get("auto_errors_window", 0),
                    "broken_now": broken
                    and last_err is not None
                    and last_err > self.cutoff,
                    # Published after the error that broke it, and no real run since
                    # (one would have moved a timestamp past the publish): a fix that
                    # has not met its trigger yet.
                    "awaiting_proof": broken and _later(published_at, last_err),
                }
            )
        return out

    def outcomes(self, conn: Connection) -> dict[str, dict[str, Any]]:
        """Latest real success and error, latest manual success, per workflow."""
        from sqlalchemy import text

        sql = (
            'SELECT "workflowId" AS wid, "status", "mode", '  # noqa: S608
            'MAX(COALESCE("stoppedAt", "startedAt")) AS at, '
            'SUM(CASE WHEN "startedAt" > :cutoff THEN 1 ELSE 0 END) AS recent '
            f"FROM {self.t('execution_entity')} "
            "WHERE \"status\" IN ('success', 'error', 'crashed') "
            + self.not_deleted(conn)
            + ' GROUP BY "workflowId", "status", "mode"'
        )
        stats: dict[str, dict[str, Any]] = {}
        rows = conn.execute(text(sql), {"cutoff": self.bound_cutoff()}).mappings()
        for row in rows:
            st = stats.setdefault(str(row["wid"]), {"auto_errors_window": 0})
            at = _utc(row["at"])
            manual = row["mode"] == "manual"
            if row["status"] == "success":
                key = "last_manual_success_at" if manual else "last_auto_success_at"
            elif manual:
                continue
            else:
                key = "last_auto_error_at"
                st["auto_errors_window"] += int(row["recent"] or 0)
            if _later(at, st.get(key)):
                st[key] = at
        return stats

    def not_deleted(self, conn: Connection) -> str:
        """The soft-delete filter, on n8n versions that soft-delete executions."""
        if "deletedAt" in self.columns(conn, "execution_entity"):
            return ' AND "deletedAt" IS NULL'
        return ""

    def executions(self, conn: Connection) -> list[dict[str, Any]]:
        """Every execution started inside the window."""
        from sqlalchemy import text

        sql = (
            'SELECT "id", "workflowId", "status", "mode", "startedAt", "stoppedAt" '  # noqa: S608
            f'FROM {self.t("execution_entity")} WHERE "startedAt" > :cutoff'
            + self.not_deleted(conn)
            + ' ORDER BY "id"'
        )
        rows = conn.execute(text(sql), {"cutoff": self.bound_cutoff()}).mappings()
        return [
            {
                "id": str(r["id"]),
                "workflow_id": str(r["workflowId"]),
                "status": r["status"],
                "mode": r["mode"],
                "started_at": _utc(r["startedAt"]),
                "stopped_at": _utc(r["stoppedAt"]),
            }
            for r in rows
        ]

    def run_errors(self, conn: Connection) -> list[dict[str, Any]]:
        """Decode the latest few real runs per workflow and report what failed.

        Capped twice: ``sample_per_workflow`` runs per workflow, and payloads over
        ``sample_max_chars`` are skipped, so the pass stays seconds on a database whose
        full execution history would take minutes to decode.
        """
        from sqlalchemy import text

        sql = (
            'SELECT "id", "workflowId", "status", "startedAt" '  # noqa: S608
            f'FROM {self.t("execution_entity")} WHERE "startedAt" > :cutoff '
            "AND \"mode\" <> 'manual' "
            "AND \"status\" IN ('success', 'error', 'crashed')"
            + self.not_deleted(conn)
            + ' ORDER BY "id" DESC'
        )
        picks: dict[str, list[dict[str, Any]]] = {}
        for row in conn.execute(text(sql), {"cutoff": self.bound_cutoff()}).mappings():
            chosen = picks.setdefault(str(row["workflowId"]), [])
            if len(chosen) < self.per_workflow:
                chosen.append(dict(row))
        payload = text(
            f'SELECT "data" FROM {self.t("execution_data")} '  # noqa: S608
            'WHERE "executionId" = :eid AND LENGTH("data") <= :cap'
        )
        out = []
        for wid, runs in picks.items():
            for run in runs:
                got = conn.execute(
                    payload, {"eid": run["id"], "cap": self.max_chars}
                ).first()
                data = _decoded(got[0]) if got else None
                if data is None:
                    continue
                failed = run["status"] in _FAILED
                found = failure_of(data) if failed else swallowed_by(data)
                if found is None:
                    continue
                node, message = found
                out.append(
                    {
                        "execution_id": str(run["id"]),
                        "workflow_id": wid,
                        "kind": "error" if failed else "swallowed",
                        "status": run["status"],
                        "started_at": _utc(run["startedAt"]),
                        "node": node,
                        "message": None if message is None else message[:_MESSAGE_CAP],
                    }
                )
        return out


def _is_trigger(node_type: str) -> bool:
    """Whether a node type starts executions (schedule, webhook, app trigger)."""
    lowered = node_type.lower()
    return any(word in lowered for word in ("trigger", "webhook", "cron"))


def _json(value: Any, default: Any) -> Any:
    """A JSON column as Python: SQLite returns text, Postgres may return it parsed."""
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return default


def _decoded(raw: Any) -> dict[str, Any] | None:
    """An execution payload decoded, or ``None`` when it is not a readable one."""
    try:
        flat = json.loads(raw) if isinstance(raw, str) else raw
        data = revive(flat) if isinstance(flat, list) else None
    except (ValueError, IndexError, TypeError):
        return None
    return data if isinstance(data, dict) else None
