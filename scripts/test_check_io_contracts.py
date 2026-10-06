import copy
import unittest

from scripts.check_io_contracts import breaking_changes, has_change_log_entry


class CompatibilityTest(unittest.TestCase):
    def setUp(self):
        self.old = {
            "families": {
                "pqbench.example": {
                    "version": 1,
                    "records": {
                        "pqbench.example:begin": {"kind": ["string"], "version": ["number"]},
                        "pqbench.example-row": {"name": ["string"]},
                    },
                }
            }
        }

    def test_additive_field_keeps_the_version(self):
        new = copy.deepcopy(self.old)
        new["families"]["pqbench.example"]["records"]["pqbench.example-row"]["size"] = ["number"]
        self.assertEqual(breaking_changes(self.old, new), [])

    def test_removed_field_and_type_change_need_a_new_version(self):
        for fields in ({}, {"name": ["number"]}):
            with self.subTest(fields=fields):
                new = copy.deepcopy(self.old)
                new["families"]["pqbench.example"]["records"]["pqbench.example-row"] = fields
                self.assertTrue(breaking_changes(self.old, new))
                new["families"]["pqbench.example"]["version"] = 2
                self.assertEqual(breaking_changes(self.old, new), [])

    def test_new_record_kind_needs_a_new_version(self):
        new = copy.deepcopy(self.old)
        new["families"]["pqbench.example"]["records"]["pqbench.example-detail"] = {"kind": ["string"]}
        self.assertTrue(breaking_changes(self.old, new))
        new["families"]["pqbench.example"]["version"] = 2
        self.assertEqual(breaking_changes(self.old, new), [])

    def test_version_cannot_decrease(self):
        new = copy.deepcopy(self.old)
        new["families"]["pqbench.example"]["version"] = 0
        self.assertTrue(breaking_changes(self.old, new))

    def test_fingerprint_needs_a_change_log_row(self):
        digest = "abc123"
        self.assertFalse(has_change_log_entry("## Contract changes\nMention abc123 in prose.\n", digest))
        self.assertTrue(has_change_log_entry("## Contract changes\n| 2026-10-06 | added field | abc123 |\n", digest))


if __name__ == "__main__":
    unittest.main()
