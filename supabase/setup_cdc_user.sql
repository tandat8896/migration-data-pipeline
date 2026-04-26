-- Supabase CDC User Setup Script
-- Run this with postgres superuser (one-time setup)
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -f setup_cdc_user.sql

\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '  Supabase CDC User Setup - Production Security'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo ''

-- ============================================================
-- STEP 1: Create replication base role
-- ============================================================
\echo '📌 Step 1: Creating replication_base role...'

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replication_base') THEN
        CREATE ROLE replication_base;
        ALTER ROLE replication_base WITH REPLICATION;
        RAISE NOTICE 'Created role: replication_base';
    ELSE
        RAISE NOTICE 'Role replication_base already exists';
    END IF;
END
$$;

-- ============================================================
-- STEP 2: Create dedicated CDC user
-- ============================================================
\echo ''
\echo '📌 Step 2: Creating debezium_cdc user...'
\echo '⚠️  IMPORTANT: Change the password below before running!'
\echo ''

-- TODO: CHANGE THIS PASSWORD!
DO $$
DECLARE
    cdc_password TEXT := 'CHANGE_ME_TO_STRONG_PASSWORD_32_CHARS';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium_cdc') THEN
        EXECUTE format('CREATE USER debezium_cdc WITH PASSWORD %L', cdc_password);
        GRANT replication_base TO debezium_cdc;
        ALTER ROLE debezium_cdc WITH LOGIN REPLICATION;
        RAISE NOTICE 'Created user: debezium_cdc';
    ELSE
        RAISE NOTICE 'User debezium_cdc already exists';
        -- Update password if user exists
        EXECUTE format('ALTER USER debezium_cdc WITH PASSWORD %L', cdc_password);
        RAISE NOTICE 'Updated password for debezium_cdc';
    END IF;
END
$$;

-- ============================================================
-- STEP 3: Grant schema access
-- ============================================================
\echo ''
\echo '📌 Step 3: Granting schema permissions...'

GRANT USAGE ON SCHEMA public TO debezium_cdc;
GRANT CONNECT ON DATABASE postgres TO debezium_cdc;

-- ============================================================
-- STEP 4: Grant table-level SELECT permissions
-- ============================================================
\echo ''
\echo '📌 Step 4: Granting SELECT on migration tables...'

GRANT SELECT ON public.customers TO debezium_cdc;
GRANT SELECT ON public.orders TO debezium_cdc;
GRANT SELECT ON public.order_items TO debezium_cdc;

-- ============================================================
-- STEP 5: Configure SSL requirement
-- ============================================================
\echo ''
\echo '📌 Step 5: Enforcing SSL for debezium_cdc...'

ALTER ROLE debezium_cdc SET ssl TO 'on';

-- ============================================================
-- STEP 6: Enable connection logging
-- ============================================================
\echo ''
\echo '📌 Step 6: Enabling connection logging...'

ALTER ROLE debezium_cdc SET log_connections TO 'on';
ALTER ROLE debezium_cdc SET log_disconnections TO 'on';

-- ============================================================
-- STEP 7: Verify setup
-- ============================================================
\echo ''
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '  ✅ Verification'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo ''

\echo '1. User attributes:'
SELECT
    rolname as "User",
    rolsuper as "Superuser",
    rolreplication as "Replication",
    rolconnlimit as "Conn Limit"
FROM pg_roles
WHERE rolname = 'debezium_cdc';

\echo ''
\echo '2. Table permissions:'
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'debezium_cdc'
ORDER BY table_name, privilege_type;

\echo ''
\echo '3. Publication info:'
SELECT
    pub.pubname,
    rol.rolname as owner,
    pub.puballtables
FROM pg_publication pub
JOIN pg_roles rol ON pub.pubowner = rol.oid
WHERE pub.pubname = 'migration_pub';

\echo ''
\echo 'Note: Publication owner is "postgres" - this is CORRECT!'
\echo '      User debezium_cdc does NOT need to own the publication.'
\echo ''

\echo '4. Publication tables:'
SELECT
    pubname,
    schemaname,
    tablename
FROM pg_publication_tables
WHERE pubname = 'migration_pub'
ORDER BY tablename;

\echo ''
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '  📝 Next Steps'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo ''
\echo '1. Test connection:'
\echo '   psql "postgresql://debezium_cdc:PASSWORD@db.xxx.supabase.co:6543/postgres?sslmode=require"'
\echo ''
\echo '2. Update Debezium config:'
\echo '   Edit: Migration_DB_Foundation/debezium/conf/pg-supabase-migration.properties'
\echo '   Set:'
\echo '     debezium.source.database.user=debezium_cdc'
\echo '     debezium.source.database.password=${SUPABASE_CDC_PASSWORD}'
\echo '     debezium.source.database.hostname=db.xxx.supabase.co'
\echo '     debezium.source.database.port=6543'
\echo ''
\echo '3. Run Debezium connector:'
\echo '   cd Migration_DB_Foundation/debezium'
\echo '   ./run.sh conf/pg-supabase-migration.properties'
\echo ''
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
