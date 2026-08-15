import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from idpe_cache import build_idp_entry_index, merge_refreshed_idpe_entry  # noqa: E402


class RefreshIdpeMappingTests(unittest.TestCase):
    def test_merges_a_stale_valid_idpe_into_the_source_canonical_idpe(self):
        canonical = "HCH8NDMR1CA4FCA"
        stale = "HCH8NDMRA4D77D"
        old_idp = "HCH8NDMRA2D542"
        new_idp = "HCH8NDMRA4D77D"
        idpe_map = {
            canonical: {
                "idpe": canonical,
                "year_idps": {"2025": old_idp},
                "year_events": {"2025": "VL_HCH8NDMR2500"},
            },
            stale: {
                "idpe": stale,
                "year_idps": {"2026": new_idp},
                "year_events": {"2026": "VL_HCH8NDMR2600"},
            },
        }
        index = build_idp_entry_index(idpe_map)
        refreshed = {
            "idpe": canonical,
            "year_idps": {"2025": old_idp, "2026": new_idp},
            "year_events": {
                "2025": "VL_HCH8NDMR2500",
                "2026": "VL_HCH8NDMR2600",
            },
        }

        changed = merge_refreshed_idpe_entry(idpe_map, canonical, refreshed, index)

        self.assertTrue(changed)
        self.assertNotIn(stale, idpe_map)
        self.assertEqual(idpe_map[canonical], refreshed)
        self.assertEqual(index[new_idp], canonical)

    def test_only_moves_results_confirmed_by_the_refreshed_source(self):
        canonical = "CANONICAL"
        stale = "STALE"
        moved_idp = "MOVED_RESULT"
        unrelated_idp = "UNRELATED_RESULT"
        idpe_map = {
            stale: {
                "idpe": stale,
                "year_idps": {"2024": unrelated_idp, "2026": moved_idp},
                "year_events": {"2024": "EVENT24", "2026": "EVENT26"},
            }
        }
        index = build_idp_entry_index(idpe_map)
        refreshed = {
            "idpe": canonical,
            "year_idps": {"2026": moved_idp},
            "year_events": {"2026": "EVENT26"},
        }

        merge_refreshed_idpe_entry(idpe_map, canonical, refreshed, index)

        self.assertEqual(idpe_map[stale]["year_idps"], {"2024": unrelated_idp})
        self.assertEqual(idpe_map[stale]["year_events"], {"2024": "EVENT24"})
        self.assertEqual(idpe_map[canonical], refreshed)
        self.assertEqual(index[unrelated_idp], stale)
        self.assertEqual(index[moved_idp], canonical)

    def test_failed_refresh_does_not_replace_a_valid_cached_mapping(self):
        canonical = "CANONICAL"
        idp = "RESULT_ID"
        original = {
            "idpe": canonical,
            "year_idps": {"2026": idp},
            "year_events": {"2026": "EVENT26"},
        }
        idpe_map = {canonical: dict(original)}
        index = build_idp_entry_index(idpe_map)
        failed = {
            "idpe": None,
            "year_idps": {"2026": idp},
            "year_events": {"2026": "EVENT26"},
        }

        changed = merge_refreshed_idpe_entry(idpe_map, idp, failed, index)

        self.assertFalse(changed)
        self.assertEqual(idpe_map, {canonical: original})
        self.assertEqual(index[idp], canonical)


if __name__ == "__main__":
    unittest.main()
