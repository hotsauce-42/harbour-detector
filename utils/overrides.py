"""
Manual property overrides for detected harbours.

The Streamlit GUI lets an operator correct a harbour's city, region and country.
Those corrections are written back into the output GeoJSON and recorded in a
``manual_overrides`` property that lists exactly which fields were touched.

Phase 5 reads that list back off the existing harbour database: when a freshly
detected cluster matches an existing harbour, the overridden fields are carried
over in place of the freshly geocoded values, so a manual correction survives
the next pipeline run. Fields that were never edited still refresh normally.

The harbour_id is never editable — it is what the match is keyed on.
"""

from typing import Any, Mapping, Optional

import pycountry

# Editable property name → the label the GUI shows for it.
EDITABLE_FIELDS: dict[str, str] = {
    "nearest_city": "City",
    "admin1":       "Region",
    "country_name": "Country",
}

# Fields derived from an editable one. They ride along with their parent both
# when saving and when carrying an override forward, so the pair cannot drift.
DEPENDENT_FIELDS: dict[str, tuple[str, ...]] = {
    "country_name": ("country_iso2",),
}

OVERRIDES_KEY = "manual_overrides"


def _is_missing(value: Any) -> bool:
    """True for None and for the NaN that Parquet/pandas use for null strings."""
    return value is None or (isinstance(value, float) and value != value)


def normalise_overrides(value: Any) -> list[str]:
    """
    Coerce a stored ``manual_overrides`` value into a clean list of field names.

    Accepts what the different round-trips hand back — a JSON list, a NumPy
    array from Parquet, a bare string, or a null — and drops anything that is
    not a currently editable field.
    """
    if _is_missing(value):
        return []
    if isinstance(value, str):
        candidates: list[Any] = [value]
    else:
        try:
            candidates = list(value)
        except TypeError:
            return []
    seen: list[str] = []
    for item in candidates:
        name = str(item)
        if name in EDITABLE_FIELDS and name not in seen:
            seen.append(name)
    # Keep a stable order so re-saving an unchanged harbour is a no-op diff.
    return [f for f in EDITABLE_FIELDS if f in seen]


def resolve_country_iso2(country_name: str) -> Optional[str]:
    """
    Best-effort ISO 3166-1 alpha-2 lookup for a country name.

    Returns None when the name cannot be resolved, so the caller can leave the
    existing code untouched rather than guessing.
    """
    name = (country_name or "").strip()
    if not name:
        return None
    country = pycountry.countries.get(name=name) or pycountry.countries.get(
        common_name=name
    )
    if country is None:
        try:
            matches = pycountry.countries.search_fuzzy(name)
        except LookupError:
            return None
        country = matches[0] if matches else None
    return getattr(country, "alpha_2", None)


def override_values(row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Extract the manually corrected values from an existing-harbour record.

    Returns ``{field: value}`` for every field the record marks as overridden,
    plus any dependent field (country_iso2) that travels with it. Fields that
    are marked but hold no value are skipped — an override that lost its value
    is not worth propagating over a freshly geocoded one.
    """
    values: dict[str, Any] = {}
    for field in normalise_overrides(row.get(OVERRIDES_KEY)):
        value = row.get(field)
        if _is_missing(value):
            continue
        values[field] = value
        for dependent in DEPENDENT_FIELDS.get(field, ()):
            dependent_value = row.get(dependent)
            if not _is_missing(dependent_value):
                values[dependent] = dependent_value
    return values
