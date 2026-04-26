-- Supabase CDC Setup Verification Script
-- Run this with debezium_cdc user to verify permissions
--
-- Usage:
--   psql "postgresql://debezium_cdc:PASSWORD@db.xxx.supabase.co:6543/postgres?sslmode=require" -f verify_cdc_setup.sql

\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '  Supabase CDC Setup Verification'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo ''

-- ============================================================
-- TEST 1: Connection info
-- ============================================================
\echo '✓ Test 1: Connection established'
SELECT
    current_user as "Connected as",
    current_database() as "Database",
    version() as "PostgreSQL Version";

\echo ''

-- ============================================================
-- TEST 2: User attributes
-- ============================================================
\echo '✓ Test 2: User attributes'
SELECT
    rolname as "User",
    rolsuper as "Is Superuser",
    rolreplication as "Can Replicate",
    rolconnlimit as "Conn Limit"
FROM pg_roles
WHERE rolname = current_user;

\echo ''

-- ============================================================
-- TEST 3: Table access - Read permissions
-- ============================================================
\echo '✓ Test 3: Table read access'

-- Check if we can read from migration tables
DO $$
DECLARE
    customer_count INT;
    order_count INT;
    item_count INT;
BEGIN
    -- Try to count customers
    BEGIN
        SELECT COUNT(*) INTO customer_count FROM customers;
        RAISE NOTICE '  ✓ customers: % rows (SELECT OK)', customer_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '  ✗ customers: Access denied - %', SQLERRM;
    END;

    -- Try to count orders
    BEGIN
        SELECT COUNT(*) INTO order_count FROM orders;
        RAISE NOTICE '  ✓ orders: % rows (SELECT OK)', order_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '  ✗ orders: Access denied - %', SQLERRM;
    END;

    -- Try to count order_items
    BEGIN
        SELECT COUNT(*) INTO item_count FROM order_items;
        RAISE NOTICE '  ✓ order_items: % rows (SELECT OK)', item_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '  ✗ order_items: Access denied - %', SQLERRM;
    END;
END;
$$;

\echo ''

-- ============================================================
-- TEST 4: Write permissions (should FAIL - read-only user)
-- ============================================================
\echo '✓ Test 4: Write restrictions (expect failures - this is good!)'

DO $$
BEGIN
    -- Try to insert (should fail)
    BEGIN
        INSERT INTO customers (email, full_name) VALUES ('test@security.com', 'Security Test');
        RAISE WARNING '  ✗ SECURITY ISSUE: INSERT succeeded (should be denied!)';
        ROLLBACK;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE '  ✓ INSERT denied (expected - read-only user)';
    WHEN OTHERS THEN
        RAISE NOTICE '  ✓ INSERT denied: %', SQLERRM;
    END;

    -- Try to update (should fail)
    BEGIN
        UPDATE customers SET full_name = 'Hacked' WHERE customer_id = 1;
        RAISE WARNING '  ✗ SECURITY ISSUE: UPDATE succeeded (should be denied!)';
        ROLLBACK;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE '  ✓ UPDATE denied (expected - read-only user)';
    WHEN OTHERS THEN
        RAISE NOTICE '  ✓ UPDATE denied: %', SQLERRM;
    END;

    -- Try to delete (should fail)
    BEGIN
        DELETE FROM customers WHERE customer_id = 1;
        RAISE WARNING '  ✗ SECURITY ISSUE: DELETE succeeded (should be denied!)';
        ROLLBACK;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE '  ✓ DELETE denied (expected - read-only user)';
    WHEN OTHERS THEN
        RAISE NOTICE '  ✓ DELETE denied: %', SQLERRM;
    END;
END;
$$;

\echo ''

-- ============================================================
-- TEST 5: Publication access
-- ============================================================
\echo '✓ Test 5: Publication access'
SELECT
    pubname as "Publication",
    schemaname as "Schema",
    tablename as "Table"
FROM pg_publication_tables
WHERE pubname = 'migration_pub'
ORDER BY tablename;

\echo ''

-- ============================================================
-- TEST 6: Replication slot (if exists)
-- ============================================================
\echo '✓ Test 6: Replication slots'
SELECT
    slot_name as "Slot Name",
    plugin as "Plugin",
    slot_type as "Type",
    active as "Active",
    CASE
        WHEN confirmed_flush_lsn IS NOT NULL
        THEN pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
        ELSE 'N/A'
    END as "Lag"
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%'
ORDER BY slot_name;

\echo ''

-- ============================================================
-- TEST 7: SSL connection
-- ============================================================
\echo '✓ Test 7: SSL/TLS encryption'
SELECT
    CASE
        WHEN ssl = true THEN '✓ SSL enabled'
        ELSE '✗ SSL NOT enabled (SECURITY RISK!)'
    END as "SSL Status",
    version as "TLS Version",
    cipher as "Cipher"
FROM pg_stat_ssl
WHERE pid = pg_backend_pid();

\echo ''

-- ============================================================
-- TEST 8: Active connections from this user
-- ============================================================
\echo '✓ Test 8: Active connections'
SELECT
    usename as "User",
    application_name as "Application",
    client_addr as "Client IP",
    state as "State",
    query_start as "Query Start"
FROM pg_stat_activity
WHERE usename = current_user
ORDER BY query_start DESC
LIMIT 5;

\echo ''

-- ============================================================
-- SUMMARY
-- ============================================================
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo '  Summary'
\echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
\echo ''

DO $$
DECLARE
    can_replicate BOOLEAN;
    ssl_enabled BOOLEAN;
    customer_count INT;
BEGIN
    -- Check replication permission
    SELECT rolreplication INTO can_replicate
    FROM pg_roles
    WHERE rolname = current_user;

    -- Check SSL
    SELECT ssl INTO ssl_enabled
    FROM pg_stat_ssl
    WHERE pid = pg_backend_pid();

    -- Check table access
    SELECT COUNT(*) INTO customer_count FROM customers;

    -- Summary
    RAISE NOTICE '';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE ' Security Checklist:';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';

    IF can_replicate THEN
        RAISE NOTICE ' ✓ Replication permission: Granted';
    ELSE
        RAISE WARNING ' ✗ Replication permission: DENIED (Required for CDC!)';
    END IF;

    IF ssl_enabled THEN
        RAISE NOTICE ' ✓ SSL/TLS encryption: Enabled';
    ELSE
        RAISE WARNING ' ✗ SSL/TLS encryption: DISABLED (SECURITY RISK!)';
    END IF;

    IF customer_count > 0 THEN
        RAISE NOTICE ' ✓ Table access: Can read % customers', customer_count;
    ELSE
        RAISE WARNING ' ⚠ Table access: Tables are empty (generate data first)';
    END IF;

    RAISE NOTICE ' ✓ User: % (non-superuser, read-only)', current_user;
    RAISE NOTICE '';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE ' Status: Ready for Debezium CDC';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE '';
END;
$$;
