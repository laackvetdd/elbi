"""Flip counting over keyed breach readings."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from elbi_core import count_flips, flapping_keys

KEYS = st.sampled_from(["a", "b", "c", "d"])
READINGS = st.lists(st.frozensets(KEYS), max_size=30)


def test_a_row_failing_on_and_off_flips_each_time_it_changes() -> None:
    readings = [set(), {"crm"}, set(), {"crm"}, {"crm"}, set()]
    assert count_flips(readings) == {"crm": 4}


def test_the_first_reading_is_the_baseline_not_a_flip() -> None:
    assert count_flips([{"compactor"}, {"compactor"}]) == {}
    assert count_flips([{"compactor"}]) == {}
    assert count_flips([]) == {}


def test_a_swap_flips_both_rows() -> None:
    assert count_flips([{"a"}, {"b"}]) == {"a": 1, "b": 1}


def test_only_rows_at_or_over_the_threshold_flap() -> None:
    readings = [set(), {"crm", "lead"}, {"lead"}, {"crm", "lead"}, {"lead"}]
    assert flapping_keys(readings, min_flips=3) == {"crm": 4}
    assert flapping_keys(readings, min_flips=4) == {"crm": 4}
    assert flapping_keys(readings, min_flips=5) == {}
    assert flapping_keys(readings, min_flips=1) == {"crm": 4, "lead": 1}


def test_a_threshold_below_one_is_refused() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        flapping_keys([], min_flips=0)


@given(READINGS)
def test_flips_never_exceed_the_gaps_between_readings(
    readings: list[frozenset[str]],
) -> None:
    gaps = max(len(readings) - 1, 0)
    assert all(0 < n <= gaps for n in count_flips(readings).values())


@given(READINGS)
def test_a_key_ends_where_its_parity_says(readings: list[frozenset[str]]) -> None:
    # An even number of flips returns a key to where it started, odd moves it.
    if not readings:
        return
    for key, flips in count_flips(readings).items():
        started, ended = key in readings[0], key in readings[-1]
        assert (started != ended) == (flips % 2 == 1)


@given(READINGS, st.integers(min_value=1, max_value=10))
def test_raising_the_threshold_only_drops_keys(
    readings: list[frozenset[str]], threshold: int
) -> None:
    loose = flapping_keys(readings, min_flips=threshold)
    strict = flapping_keys(readings, min_flips=threshold + 1)
    assert strict.items() <= loose.items()
    assert list(loose) == sorted(loose)
