"""Helpers for reconciling cached MIKA person IDs with refreshed source data."""


def build_idp_entry_index(idpe_map: dict) -> dict[str, str]:
    """Build a reverse index from result ID (idp) to cached person-map key."""
    result = {}
    for key, entry in idpe_map.items():
        for idp in entry.get("year_idps", {}).values():
            if isinstance(idp, str):
                result[idp] = key
    return result


def merge_refreshed_idpe_entry(
    idpe_map: dict,
    key: str,
    entry: dict,
    idp_to_entry_key: dict[str, str],
) -> bool:
    """Merge a freshly resolved source identity into the persistent cache.

    A result can move from one valid ``idpe`` to another when MIKA corrects
    participant data. Move only result IDs confirmed by the fresh detail and
    history pages; unrelated results under the old identity remain untouched.
    A failed refresh never replaces an existing valid mapping with ``None``.
    """
    fresh_idpe = entry.get("idpe")
    fresh_year_idps = entry.get("year_idps", {})

    if not fresh_idpe:
        cached_keys = {
            idp_to_entry_key[idp]
            for idp in fresh_year_idps.values()
            if idp in idp_to_entry_key
        }
        if any(idpe_map.get(old_key, {}).get("idpe") for old_key in cached_keys):
            return False
        idpe_map[key] = entry
        for idp in fresh_year_idps.values():
            if isinstance(idp, str):
                idp_to_entry_key[idp] = key
        return True

    canonical_key = fresh_idpe

    # Detach only the freshly confirmed results from stale identities. An old
    # identity may still own other results that were not part of this refresh.
    for idp in fresh_year_idps.values():
        old_key = idp_to_entry_key.get(idp)
        if not old_key or old_key == canonical_key:
            continue
        old_entry = idpe_map.get(old_key)
        if not old_entry:
            continue
        removed_years = [
            year
            for year, old_idp in old_entry.get("year_idps", {}).items()
            if old_idp == idp
        ]
        for year in removed_years:
            old_entry.get("year_idps", {}).pop(year, None)
            old_entry.get("year_events", {}).pop(year, None)
        if old_entry.get("year_idps"):
            idpe_map[old_key] = old_entry
        else:
            idpe_map.pop(old_key, None)
        if idp_to_entry_key.get(idp) == old_key:
            idp_to_entry_key.pop(idp, None)

    existing = idpe_map.get(canonical_key, {})
    merged = dict(existing)
    merged.update({k: v for k, v in entry.items() if k not in ("year_idps", "year_events")})
    merged["idpe"] = fresh_idpe
    merged_year_idps = dict(existing.get("year_idps", {}))
    merged_year_events = dict(existing.get("year_events", {}))

    # If a refreshed year now names a different result, discard the obsolete
    # reverse-index entry before the fresh mapping wins.
    for year, idp in fresh_year_idps.items():
        replaced_idp = merged_year_idps.get(year)
        if replaced_idp != idp and idp_to_entry_key.get(replaced_idp) == canonical_key:
            idp_to_entry_key.pop(replaced_idp, None)

    merged_year_idps.update(fresh_year_idps)
    merged_year_events.update(entry.get("year_events", {}))
    merged["year_idps"] = merged_year_idps
    merged["year_events"] = merged_year_events
    idpe_map[canonical_key] = merged

    for idp in merged_year_idps.values():
        if isinstance(idp, str):
            idp_to_entry_key[idp] = canonical_key
    return True
