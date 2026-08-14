"""Unit tests for the manual-override semantics shared by the GUI and Phase 5."""

import numpy as np
import pandas as pd

from utils.overrides import (
    EDITABLE_FIELDS,
    OVERRIDES_KEY,
    normalise_overrides,
    override_values,
    resolve_country_iso2,
)


# ---------------------------------------------------------------------------
# normalise_overrides — has to survive every round-trip the data takes
# ---------------------------------------------------------------------------

def test_normalise_handles_missing_values():
    assert normalise_overrides(None) == []
    assert normalise_overrides(float("nan")) == []
    assert normalise_overrides([]) == []


def test_normalise_accepts_numpy_array_from_parquet():
    """Parquet list<string> columns read back as ndarray, not list."""
    got = normalise_overrides(np.array(["nearest_city", "admin1"]))
    assert got == ["nearest_city", "admin1"]


def test_normalise_accepts_bare_string():
    assert normalise_overrides("admin1") == ["admin1"]


def test_normalise_drops_unknown_fields():
    """A field that is no longer editable must not be carried forward."""
    assert normalise_overrides(["n_events", "admin1", "harbour_id"]) == ["admin1"]


def test_normalise_is_order_stable_and_deduplicated():
    a = normalise_overrides(["country_name", "nearest_city", "country_name"])
    b = normalise_overrides(["nearest_city", "country_name"])
    assert a == b == ["nearest_city", "country_name"]


def test_harbour_id_is_not_editable():
    assert "harbour_id" not in EDITABLE_FIELDS


# ---------------------------------------------------------------------------
# resolve_country_iso2
# ---------------------------------------------------------------------------

def test_resolve_country_common_names():
    assert resolve_country_iso2("Germany") == "DE"
    assert resolve_country_iso2("Netherlands") == "NL"
    assert resolve_country_iso2("South Korea") == "KR"


def test_resolve_country_is_case_insensitive():
    assert resolve_country_iso2("germany") == "DE"


def test_resolve_country_returns_none_for_nonsense():
    """Unresolvable names must return None so the caller keeps the old code."""
    assert resolve_country_iso2("Freedonia") is None
    assert resolve_country_iso2("") is None
    assert resolve_country_iso2(None) is None


# ---------------------------------------------------------------------------
# override_values
# ---------------------------------------------------------------------------

def test_override_values_only_returns_marked_fields():
    row = {
        OVERRIDES_KEY:  ["nearest_city"],
        "nearest_city": "Hamburg-Altona",
        "admin1":       "Hamburg",
        "country_name": "Germany",
    }
    assert override_values(row) == {"nearest_city": "Hamburg-Altona"}


def test_override_values_carries_iso2_with_country_name():
    row = {
        OVERRIDES_KEY:  ["country_name"],
        "country_name": "Germany",
        "country_iso2": "DE",
    }
    assert override_values(row) == {"country_name": "Germany",
                                    "country_iso2": "DE"}


def test_override_values_skips_marked_but_empty_field():
    """A marker with no value must not overwrite a freshly geocoded value."""
    row = {OVERRIDES_KEY: ["nearest_city"], "nearest_city": None}
    assert override_values(row) == {}


def test_override_values_works_on_pandas_row():
    """Phase 5 feeds this rows from iterrows(), not plain dicts."""
    df = pd.DataFrame([{
        OVERRIDES_KEY:  ["admin1"],
        "admin1":       "Schleswig-Holstein",
        "nearest_city": "Hamburg",
    }])
    _, row = next(df.iterrows())
    assert override_values(row) == {"admin1": "Schleswig-Holstein"}


def test_override_values_empty_when_nothing_marked():
    row = {"nearest_city": "Hamburg", "admin1": "Hamburg"}
    assert override_values(row) == {}
