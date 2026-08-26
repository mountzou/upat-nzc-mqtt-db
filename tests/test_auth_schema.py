"""Static safety contracts for the additive application-user schema."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db" / "migrations" / "011_app_users.sql"
INIT_SQL = ROOT / "db" / "init.sql"


class AuthSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = re.sub(r"\s+", " ", cls.sql).lower()

    def test_migration_is_additive_and_has_no_seed_credentials(self):
        for forbidden in (
            " alter table ",
            " drop table ",
            " truncate ",
            " delete from ",
            " insert into ",
            " create extension ",
        ):
            self.assertNotIn(forbidden, f" {self.normalized} ")

    def test_schema_owns_identity_scope_and_revocation_state(self):
        for fragment in (
            "username text not null unique",
            "password_hash text not null",
            "role text not null",
            "school_id text",
            "municipality_id text",
            "school_ids text[] not null",
            "is_active boolean not null default true",
            "token_version bigint not null default 1",
        ):
            self.assertIn(fragment, self.normalized)

    def test_roles_and_scopes_are_constrained(self):
        self.assertIn("'teacher', 'municipality', 'system_admin'", self.normalized)
        self.assertIn("school_ids = array[school_id]", self.normalized)
        self.assertIn("cardinality(school_ids) >= 1", self.normalized)
        self.assertIn("cardinality(school_ids) = 0", self.normalized)

    def test_fresh_database_init_includes_the_migration(self):
        self.assertIn(
            r"\ir migrations/011_app_users.sql",
            INIT_SQL.read_text(encoding="utf-8"),
        )


if __name__ == "__main__":
    unittest.main()
