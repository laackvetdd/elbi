"""A keyed monitor names rows that fail on and off, once, as ``metric.flapping``.

The case this covers: a workflow fails, recovers, and fails again. Each failure only
widens or reopens a breach and each success clears it, so the monitor's alerts read as
unrelated one-offs. With ``flap`` set, the row is named as flapping once it switches
``min_flips`` times inside the window.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from elbi import notifications
from elbi.db import MetricSnapshot, Store, open_store
from elbi.monitoring import (
    BREACH_WIDENED,
    FLAPPING,
    MonitorError,
    MonitorService,
    Reading,
    flap_settings,
)

READING: dict[str, Any] = {"value": Reading(0.0, ())}
ALERTS: list[dict[str, Any]] = []
FLAP = {"window": "7d", "min_flips": 3}


@pytest.fixture(autouse=True)
def _reset() -> None:
    READING["value"] = Reading(0.0, ())
    ALERTS.clear()


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return open_store(f"sqlite:{tmp_path / 'app.db'}")


def _service(store: Store) -> MonitorService:
    return MonitorService(
        store=store,
        read_value=lambda monitor: READING["value"],
        source_certified=lambda kind, target: True,
        on_alert=ALERTS.append,
    )


def _monitor(service: MonitorService, flap: Any = FLAP) -> str:
    config: dict[str, Any] = {
        "measure": {"column": "quiet_breach", "agg": "sum", "key": "name"}
    }
    if flap is not None:
        config["flap"] = flap
    return service.create(
        name="quiet failures",
        target_kind="derivation",
        target="workflow_health",
        config=config,
        max_value=0,
        window=2,
    ).id


def _read(service: MonitorService, monitor_id: str, *keys: str) -> dict[str, Any]:
    READING["value"] = Reading(float(len(keys)), tuple(sorted(keys)))
    return service.check(monitor_id)


def _flapping(alerts: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    return [a for a in (ALERTS if alerts is None else alerts) if a["event"] == FLAPPING]


def test_a_row_failing_on_and_off_is_named_once(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service)
    # "lead" stays broken, which keeps one incident open the whole time, so the CRM
    # sync failing on and off surfaces only as widenings, never as a pattern.
    _read(service, monitor_id, "lead")
    _read(service, monitor_id, "lead", "crm")  # flip 1
    _read(service, monitor_id, "lead")  # flip 2
    assert _flapping() == []
    result = _read(service, monitor_id, "lead", "crm")  # flip 3
    assert result["alerted"] is True
    [alert] = _flapping()
    assert alert["new_keys"] == ["crm"]
    assert alert["flapping_keys"] == ["crm"]
    assert alert["flips"] == {"crm": 3}
    assert alert["window_hours"] == 168
    # Still flapping on the next flips: no second alert.
    _read(service, monitor_id, "lead")
    _read(service, monitor_id, "lead", "crm")
    assert len(_flapping()) == 1
    assert BREACH_WIDENED in [a["event"] for a in ALERTS]


def test_a_steadily_broken_row_never_flaps(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service)
    _read(service, monitor_id)
    for _ in range(6):
        _read(service, monitor_id, "compactor")
    assert _flapping() == []


def test_a_second_row_starting_to_flap_alerts_for_that_row_only(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service, {"window": "2d", "min_flips": 2})
    _read(service, monitor_id, "lead")
    _read(service, monitor_id, "lead", "crm")
    _read(service, monitor_id, "lead")  # crm: 2 flips
    _read(service, monitor_id, "lead", "media")
    _read(service, monitor_id, "lead")  # media: 2 flips
    alerts = _flapping()
    assert [a["new_keys"] for a in alerts] == [["crm"], ["media"]]
    assert alerts[1]["flapping_keys"] == ["crm", "media"]


def test_flips_outside_the_window_do_not_count(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service, {"window": "1h", "min_flips": 2})
    old = datetime.now(UTC) - timedelta(hours=2)
    for i, keys in enumerate((["crm"], [], ["crm"], [])):
        store.add_metric_snapshot(
            MetricSnapshot(
                monitor_id=monitor_id,
                value=float(len(keys)),
                at=old + timedelta(minutes=i),
                breach_keys=json.dumps(keys),
            )
        )
    _read(service, monitor_id)
    _read(service, monitor_id, "media")
    _read(service, monitor_id)
    # crm flipped 3 times two hours ago; only media flipped inside the hour.
    [alert] = _flapping()
    assert alert["flapping_keys"] == ["media"]
    assert alert["flips"] == {"media": 2}


def test_unkeyed_snapshots_are_ignored_by_the_flip_count(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service, {"window": "7d", "min_flips": 2})
    _read(service, monitor_id, "crm")
    store.add_metric_snapshot(MetricSnapshot(monitor_id=monitor_id, value=0.0))
    _read(service, monitor_id, "crm")
    _read(service, monitor_id)
    _read(service, monitor_id, "crm")
    [alert] = _flapping()
    assert alert["flips"] == {"crm": 2}


def test_a_restart_does_not_realert_a_row_already_flapping(store: Store) -> None:
    first = _service(store)
    monitor_id = _monitor(first, {"window": "7d", "min_flips": 2})
    _read(first, monitor_id)
    _read(first, monitor_id, "crm")
    _read(first, monitor_id)
    assert len(_flapping()) == 1
    restarted = _service(store)
    _read(restarted, monitor_id, "crm")
    _read(restarted, monitor_id)
    assert len(_flapping()) == 1
    # The persisted history still counts: a new row starting to flap alerts at once.
    _read(restarted, monitor_id, "media")
    _read(restarted, monitor_id)
    assert [a["new_keys"] for a in _flapping()] == [["crm"], ["media"]]


def test_a_monitor_without_flap_never_raises_it(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service, flap=None)
    for keys in ((), ("crm",), (), ("crm",), (), ("crm",)):
        _read(service, monitor_id, *keys)
    assert _flapping() == []


def test_the_breaching_keys_are_stored_on_each_snapshot(store: Store) -> None:
    service = _service(store)
    monitor_id = _monitor(service)
    _read(service, monitor_id, "b", "a")
    [snap] = store.list_metric_snapshots(monitor_id)
    assert snap.breach_keys == '["a", "b"]'


@pytest.mark.parametrize(
    ("flap", "expected"),
    [
        ({"window": "7d", "min_flips": 3}, (168.0, 3)),
        ({"window": "36h"}, (36.0, 3)),
        ({"window": 12, "min_flips": 2}, (12.0, 2)),
        ({}, (168.0, 3)),
    ],
)
def test_flap_settings_read_the_window_and_threshold(
    flap: dict[str, Any], expected: tuple[float, int]
) -> None:
    assert flap_settings({"flap": flap}) == expected
    assert flap_settings({}) is None


@pytest.mark.parametrize(
    "flap",
    [
        "7d",
        {"window": "soon"},
        {"window": "0d"},
        {"window": "-1"},
        {"min_flips": 0},
        {"min_flips": "x"},
    ],
)
def test_a_malformed_flap_is_refused_when_saved(store: Store, flap: Any) -> None:
    service = _service(store)
    with pytest.raises(MonitorError):
        _monitor(service, flap)
    good = _monitor(service)
    with pytest.raises(MonitorError):
        service.update(good, config={"flap": flap})


def test_a_flapping_row_notifies_and_emails_by_default(store: Store) -> None:
    assert notifications.EVENT_TYPES[FLAPPING] is True
    notifications.create_notifications(
        store,
        FLAPPING,
        {
            "event": FLAPPING,
            "monitor_id": "m1",
            "monitor": "quiet failures",
            "new_keys": ["crm"],
            "reason": "crm switched between breaching and clear 3 or more times "
            "in 168h",
        },
    )
    [row] = store.list_notifications()
    assert row.title == "Monitor 'quiet failures' has flapping rows"
    assert row.body.startswith("Failing on and off: crm. crm switched")
