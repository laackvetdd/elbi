"""The n8n connector, against a SQLite file laid out the way n8n lays out its own.

The fixture holds one workflow per behavior worth telling apart: healthy, broken with
no error workflow, fixed and published but not yet proven by a real run, fixed and
proven, broken only by a manual test, and one whose successful run swallowed a node
error. Each test reads through ``extract`` (the public surface the sync layer drives),
so the SQL, the timestamp handling and the payload decoding are all exercised together.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from elbi.warehouse.sources.base import SourceInputs
from elbi.warehouse.sources.n8n import (
    N8nSource,
    failure_of,
    revive,
    swallowed_by,
)
from elbi.warehouse.sources.registry import SourceRegistry

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def _ts(hours_ago: float) -> str:
    """A timestamp in n8n's SQLite text layout, ``hours_ago`` before NOW."""
    moment = NOW - timedelta(hours=hours_ago)
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def flatten(value: Any) -> list[Any]:
    """Encode ``value`` the way n8n's ``flatted`` does (strings become references)."""
    out: list[Any] = []

    def put(item: Any) -> str:
        out.append(None)
        index = len(out) - 1
        if isinstance(item, dict):
            out[index] = {k: ref(v) for k, v in item.items()}
        elif isinstance(item, list):
            out[index] = [ref(v) for v in item]
        else:
            out[index] = item
        return str(index)

    def ref(item: Any) -> Any:
        if isinstance(item, (dict, list, str)):
            return put(item)
        return item

    put(value)
    return out


def _errored(node: str, message: str) -> str:
    data = {
        "resultData": {
            "error": {"message": message, "node": {"name": node}},
            "lastNodeExecuted": node,
            "runData": {},
        }
    }
    return json.dumps(flatten(data))


def _clean() -> str:
    data = {
        "resultData": {
            "runData": {"Fetch": [{"data": {"main": [[{"json": {"ok": 1}}]]}}]}
        }
    }
    return json.dumps(flatten(data))


def _swallowed(node: str, message: str) -> str:
    item = {"json": {"error": {"message": message}}}
    data = {"resultData": {"runData": {node: [{"data": {"main": [[item]]}}]}}}
    return json.dumps(flatten(data))


# (id, name, settings, published hours ago, runs as (status, mode, hours ago, data))
WORKFLOWS: list[
    tuple[str, str, dict[str, Any], float, list[tuple[str, str, float, str | None]]]
] = [
    (
        "w-ok",
        "Healthy",
        {"errorWorkflow": "w-alert"},
        200,
        [
            ("error", "webhook", 30, _errored("HTTP", "timeout")),
            ("success", "webhook", 2, _clean()),
        ],
    ),
    (
        "w-broken",
        "Lead capture",
        {},
        200,
        [
            ("success", "webhook", 40, _clean()),
            ("error", "webhook", 3, _errored("HubSpot", "401 Unauthorized")),
        ],
    ),
    (
        "w-awaiting",
        "CRM sync",
        {},
        1,
        [
            ("success", "trigger", 50, _clean()),
            ("error", "trigger", 5, _errored("Map", "bad enum")),
            ("success", "manual", 0.5, _clean()),
        ],
    ),
    (
        "w-proven",
        "Fixed and run",
        {},
        4,
        [
            ("error", "trigger", 6, _errored("Map", "bad enum")),
            ("success", "trigger", 1, _clean()),
        ],
    ),
    (
        "w-manual",
        "Edited in the editor",
        {},
        200,
        [
            ("success", "webhook", 10, _clean()),
            ("error", "manual", 1, _errored("Code", "typo")),
        ],
    ),
    (
        "w-swallow",
        "Feedback sync",
        {},
        200,
        [
            ("success", "trigger", 2, _swallowed("Promote", "duplicate key")),
        ],
    ),
    (
        "w-old",
        "Failed long ago",
        {},
        900,
        [
            ("error", "trigger", 24 * 10, _errored("Old", "gone")),
        ],
    ),
]


@pytest.fixture
def n8n_db(tmp_path: Path) -> Path:
    """A SQLite file with n8n's table and column names."""
    path = tmp_path / "database.sqlite"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE workflow_entity (
            id TEXT PRIMARY KEY, name TEXT, active BOOLEAN, isArchived BOOLEAN,
            settings TEXT, nodes TEXT, createdAt TEXT, updatedAt TEXT,
            activeVersionId TEXT);
        CREATE TABLE workflow_history (
            versionId TEXT PRIMARY KEY, workflowId TEXT, nodes TEXT, createdAt TEXT);
        CREATE TABLE execution_entity (
            id INTEGER PRIMARY KEY, workflowId TEXT, status TEXT, mode TEXT,
            startedAt TEXT, stoppedAt TEXT, deletedAt TEXT);
        CREATE TABLE execution_data (
            executionId INTEGER PRIMARY KEY, data TEXT, workflowData TEXT);
        """
    )
    nodes = json.dumps(
        [
            {"type": "n8n-nodes-base.webhook"},
            {"type": "n8n-nodes-base.scheduleTrigger", "disabled": True},
            {"type": "n8n-nodes-base.set"},
        ]
    )
    rows: list[tuple[str, str, float, str, str | None]] = []
    for wid, name, settings, published, runs in WORKFLOWS:
        version = f"v-{wid}"
        con.execute(
            "INSERT INTO workflow_entity VALUES (?, ?, 1, 0, ?, ?, ?, ?, ?)",
            (
                wid,
                name,
                json.dumps(settings),
                nodes,
                _ts(1000),
                _ts(published),
                version,
            ),
        )
        con.execute(
            "INSERT INTO workflow_history VALUES (?, ?, ?, ?)",
            (version, wid, nodes, _ts(published)),
        )
        rows.extend((wid, status, ago, mode, data) for status, mode, ago, data in runs)
    # Insert oldest first, so execution ids rise with time as they do in n8n.
    rows.sort(key=lambda r: -r[2])
    for eid, (wid, status, ago, mode, data) in enumerate(rows, start=1):
        con.execute(
            "INSERT INTO execution_entity VALUES (?, ?, ?, ?, ?, ?, NULL)",
            (eid, wid, status, mode, _ts(ago), _ts(ago - 0.01)),
        )
        if data is not None:
            con.execute("INSERT INTO execution_data VALUES (?, ?, '{}')", (eid, data))
    # A soft-deleted execution must not count anywhere.
    con.execute(
        "INSERT INTO execution_entity VALUES "
        "(999, 'w-ok', 'error', 'webhook', ?, ?, ?)",
        (_ts(0.1), _ts(0.1), _ts(0.05)),
    )
    con.commit()
    con.close()
    return path


def _read(path: Path, table: str, **extra: Any) -> list[dict[str, Any]]:
    source = N8nSource(clock=lambda: NOW)
    config = {"backend": "sqlite", "database": str(path), **extra}
    tables = list(source.extract(SourceInputs(config=config, schema=table)))
    return [row for t in tables for row in t.to_pylist()]


def _by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {r["id"]: r for r in rows}


def test_n8n_is_in_the_catalog_with_its_three_tables(n8n_db: Path) -> None:
    source = SourceRegistry.get("n8n")
    assert source.config.category == "Engineering & monitoring"
    names = [s.name for s in source.schemas({"database": str(n8n_db)})]
    assert names == ["workflows", "executions", "run_errors"]


def test_a_broken_workflow_is_broken_now_and_not_awaiting_proof(n8n_db: Path) -> None:
    row = _by_id(_read(n8n_db, "workflows"))["w-broken"]
    assert row["broken_now"] is True
    assert row["awaiting_proof"] is False
    assert row["has_error_workflow"] is False
    assert row["auto_errors_window"] == 1


def test_a_fix_published_after_the_error_awaits_proof(n8n_db: Path) -> None:
    row = _by_id(_read(n8n_db, "workflows"))["w-awaiting"]
    # Still broken: the manual success after publish proves nothing about the trigger.
    assert row["broken_now"] is True
    assert row["awaiting_proof"] is True
    assert row["last_manual_success_at"] > row["published_at"]


def test_a_real_run_after_the_fix_clears_both_flags(n8n_db: Path) -> None:
    row = _by_id(_read(n8n_db, "workflows"))["w-proven"]
    assert row["broken_now"] is False
    assert row["awaiting_proof"] is False


def test_a_manual_error_does_not_break_a_workflow(n8n_db: Path) -> None:
    row = _by_id(_read(n8n_db, "workflows"))["w-manual"]
    assert row["broken_now"] is False
    assert row["last_auto_error_at"] is None


def test_an_error_outside_the_window_is_not_broken_now(n8n_db: Path) -> None:
    rows = _by_id(_read(n8n_db, "workflows"))
    assert rows["w-old"]["broken_now"] is False
    assert rows["w-old"]["auto_errors_window"] == 0
    # A wider window brings it back.
    wide = _by_id(_read(n8n_db, "workflows", history_days=14))
    assert wide["w-old"]["broken_now"] is True


def test_a_healthy_workflow_with_an_alert_reads_clean(n8n_db: Path) -> None:
    row = _by_id(_read(n8n_db, "workflows"))["w-ok"]
    assert row["broken_now"] is False
    assert row["has_error_workflow"] is True
    assert row["error_workflow"] == "w-alert"
    # The soft-deleted error 6 minutes ago is ignored.
    assert row["last_auto_error_at"] == NOW - timedelta(hours=30 - 0.01)
    assert row["trigger_types"] == "n8n-nodes-base.webhook"
    assert row["published_at"] == NOW - timedelta(hours=200)


def test_executions_cover_the_window_only_and_skip_deleted(n8n_db: Path) -> None:
    rows = _read(n8n_db, "executions")
    ids = {r["id"] for r in rows}
    assert "999" not in ids
    assert all(r["started_at"] > NOW - timedelta(days=7) for r in rows)
    total = sum(len(runs) for *_, runs in WORKFLOWS)
    assert len(rows) == total - 1  # the 10-day-old error is outside 7 days
    assert [r["id"] for r in rows] == sorted((r["id"] for r in rows), key=int)


def test_run_errors_name_the_failing_node_and_the_swallowed_one(n8n_db: Path) -> None:
    rows = _read(n8n_db, "run_errors")
    found = {(r["workflow_id"], r["kind"], r["node"], r["message"]) for r in rows}
    assert ("w-broken", "error", "HubSpot", "401 Unauthorized") in found
    assert ("w-swallow", "swallowed", "Promote", "duplicate key") in found
    # Manual runs are never sampled, and a clean success yields no row.
    assert not any(r["workflow_id"] == "w-manual" for r in rows)
    assert not any(
        r["kind"] == "swallowed" and r["workflow_id"] == "w-ok" for r in rows
    )


def test_run_error_sampling_respects_both_caps(n8n_db: Path) -> None:
    one = _read(n8n_db, "run_errors", sample_per_workflow=1)
    # w-ok's latest real run is the clean success, so with one sample it has no row.
    assert not any(r["workflow_id"] == "w-ok" for r in one)
    tiny = _read(n8n_db, "run_errors", sample_max_chars=10)
    assert tiny == []


def test_an_unknown_table_is_refused(n8n_db: Path) -> None:
    with pytest.raises(ValueError, match="Unknown n8n table"):
        _read(n8n_db, "credentials_entity")


def test_the_database_is_opened_read_only(n8n_db: Path) -> None:
    source = N8nSource(clock=lambda: NOW)
    from elbi.warehouse.sources.n8n import _Reader

    engine = _Reader({"database": str(n8n_db)}, NOW).engine()
    with engine.connect() as conn, pytest.raises(Exception, match="readonly"):
        from sqlalchemy import text

        conn.execute(text("DELETE FROM execution_entity"))
    assert source.source_type == "n8n"


@pytest.mark.parametrize(
    ("config", "ok"),
    [
        ({"database": "/x.sqlite"}, True),
        ({"backend": "sqlite"}, False),
        ({"backend": "postgres", "database": "/x.sqlite"}, False),
        ({"backend": "postgres", "url": "postgresql://ro@h/n8n"}, True),
        ({"backend": "mysql", "url": "x"}, False),
        ({"database": "/x.sqlite", "table_prefix": "n8n_"}, True),
        ({"database": "/x.sqlite", "table_prefix": 'x"; DROP'}, False),
    ],
)
def test_validation_asks_only_for_what_the_backend_needs(
    config: dict[str, Any], ok: bool
) -> None:
    assert N8nSource().validate(config)[0] is ok


def test_postgres_urls_get_the_installed_driver() -> None:
    from elbi.warehouse.sources.n8n import _Reader

    for url in ("postgres://u:p@h/n8n", "postgresql://u:p@h/n8n"):
        engine = _Reader({"backend": "postgres", "url": url}, NOW).engine()
        assert engine.url.drivername == "postgresql+psycopg"


def test_revive_keeps_shared_references_shared() -> None:
    shared = {"name": "x"}
    flat = flatten({"a": shared, "b": [1, True, None]})
    assert revive(flat) == {"a": {"name": "x"}, "b": [1, True, None]}
    # A self-reference decodes to the same object rather than recursing forever.
    looped = revive([{"self": "0"}])
    assert looped["self"] is looped
    assert revive([]) is None


def test_failure_falls_back_to_the_last_node_executed() -> None:
    data = {"resultData": {"error": {"message": "boom"}, "lastNodeExecuted": "Tail"}}
    assert failure_of(data) == ("Tail", "boom")
    assert failure_of({}) == (None, None)


def test_a_node_run_error_beside_its_output_counts_as_swallowed() -> None:
    data = {
        "resultData": {
            "runData": {"Ok": [None], "Post": [{"error": {"message": "503"}}]}
        }
    }
    assert swallowed_by(data) == ("Post", "503")
    big = {"json": {"error": "x", "a": 1, "b": 2, "c": 3}}
    assert (
        swallowed_by({"resultData": {"runData": {"N": [{"data": {"main": [[big]]}}]}}})
        is None
    )


def test_a_prefix_that_is_not_an_identifier_never_reaches_sql(n8n_db: Path) -> None:
    with pytest.raises(ValueError, match="Invalid n8n table prefix"):
        _read(n8n_db, "workflows", table_prefix='x" --')
