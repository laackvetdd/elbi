"""Detect keyed rows that flap: breach, clear, and breach again inside a window.

A keyed monitor reads, on each check, the set of rows that breach (a workflow failing,
an account over its limit). An incident tracks the sum, so a row that fails on and off
hides inside it: each failure reopens or widens the breach and each success clears it,
and no single reading looks different from a one-off. Counting how often each row's
membership changes across the readings in a window names the pattern directly.

A flip is a change of membership between two consecutive readings: a row that joins the
breaching set flips once, and leaving again is a second flip. The first reading in the
window is the baseline, so a row breaching when the window opens has not flipped. The
functions are pure and dependency-free, like :func:`detect_anomaly`.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence


def count_flips(readings: Sequence[Collection[str]]) -> dict[str, int]:
    """How many times each key's breach membership changed, oldest reading first.

    Keys that never changed are absent from the result.
    """
    flips: dict[str, int] = {}
    previous: frozenset[str] | None = None
    for reading in readings:
        current = frozenset(reading)
        if previous is not None:
            for key in current ^ previous:
                flips[key] = flips.get(key, 0) + 1
        previous = current
    return flips


def flapping_keys(
    readings: Sequence[Collection[str]], *, min_flips: int
) -> dict[str, int]:
    """The keys that flipped at least ``min_flips`` times, with their flip counts.

    Raises:
        ValueError: if ``min_flips`` is below 1 (every key would flap).
    """
    if min_flips < 1:
        raise ValueError("min_flips must be at least 1")
    return {
        key: flips
        for key, flips in sorted(count_flips(readings).items())
        if flips >= min_flips
    }
