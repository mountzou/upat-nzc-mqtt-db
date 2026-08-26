-- Additive per-user UI preferences and successful-login tracking.
ALTER TABLE app_users
    ADD COLUMN IF NOT EXISTS theme TEXT NOT NULL DEFAULT 'light',
    ADD COLUMN IF NOT EXISTS onboarding_completed TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'app_users'::regclass
          AND conname = 'app_users_theme_check'
    ) THEN
        ALTER TABLE app_users
            ADD CONSTRAINT app_users_theme_check
            CHECK (theme IN ('light', 'dark'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'app_users'::regclass
          AND conname = 'app_users_onboarding_completed_check'
    ) THEN
        ALTER TABLE app_users
            ADD CONSTRAINT app_users_onboarding_completed_check
            CHECK (
                CARDINALITY(onboarding_completed) <= 64
                AND ARRAY_POSITION(onboarding_completed, '') IS NULL
            );
    END IF;
END
$$;

COMMENT ON COLUMN app_users.theme IS
    'Authenticated user UI theme preference.';
COMMENT ON COLUMN app_users.onboarding_completed IS
    'Versioned onboarding guide keys completed by the user.';
COMMENT ON COLUMN app_users.last_login_at IS
    'Timestamp of the most recent successful password login.';
