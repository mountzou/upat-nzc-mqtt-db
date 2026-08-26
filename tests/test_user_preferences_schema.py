"""Static contracts for additive user preferences and login tracking."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db" / "migrations" / "012_app_user_preferences.sql"
INIT_SQL = ROOT / "db" / "init.sql"


class UserPreferencesSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = re.sub(r"\s+", " ", cls.sql).lower()

    def test_migration_is_additive_and_contains_no_user_data(self):
        for forbidden in (
            " drop table ",
            " truncate ",
            " delete from ",
            " insert into ",
            " password_hash ",
        ):
            self.assertNotIn(forbidden, f" {self.normalized} ")

    def test_schema_tracks_only_the_requested_simple_user_state(self):
        for fragment in (
            "theme text not null default 'light'",
            "onboarding_completed text[] not null default array[]::text[]",
            "last_login_at timestamptz",
            "theme in ('light', 'dark')",
        ):
            self.assertIn(fragment, self.normalized)

    def test_fresh_database_init_includes_the_migration(self):
        self.assertIn(
            r"\ir migrations/012_app_user_preferences.sql",
            INIT_SQL.read_text(encoding="utf-8"),
        )


if __name__ == "__main__":
    unittest.main()
