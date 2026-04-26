-- Supabase Target Database Schema (pgcli compatible)
-- Simplified version without \echo commands

-- Drop existing tables
DROP TABLE IF EXISTS target_order_items CASCADE;
DROP TABLE IF EXISTS target_orders CASCADE;
DROP TABLE IF EXISTS target_customers CASCADE;
DROP TABLE IF EXISTS target_customer_events CASCADE;
DROP TABLE IF EXISTS target_inventory_snapshots CASCADE;
DROP TABLE IF EXISTS migration_status CASCADE;

-- Create target tables - Postgres Source
CREATE TABLE target_customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'postgres_local',
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_lsn TEXT,
    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

CREATE TABLE target_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES target_customers(customer_id) ON DELETE CASCADE,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'postgres_local',
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_lsn TEXT,
    CHECK (total_amount >= 0),
    CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'refunded'))
);

CREATE TABLE target_order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES target_orders(order_id) ON DELETE CASCADE,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'postgres_local',
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_lsn TEXT,
    CHECK (quantity > 0),
    CHECK (price >= 0)
);

-- Create target tables - MongoDB Source
CREATE TABLE target_customer_events (
    event_id TEXT PRIMARY KEY,
    customer_id INTEGER,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    event_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'mongodb_atlas',
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mongo_object_id TEXT,
    CHECK (event_type IN ('login', 'logout', 'purchase', 'view', 'cart_add', 'cart_remove'))
);

CREATE TABLE target_inventory_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    warehouse_location VARCHAR(255),
    snapshot_data JSONB,
    snapshot_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'mongodb_atlas',
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mongo_object_id TEXT,
    CHECK (quantity >= 0)
);

-- Create indexes
CREATE INDEX idx_target_customers_email ON target_customers(email);
CREATE INDEX idx_target_customers_migrated_at ON target_customers(migrated_at);
CREATE INDEX idx_target_orders_customer_id ON target_orders(customer_id);
CREATE INDEX idx_target_orders_status ON target_orders(status);
CREATE INDEX idx_target_orders_created_at ON target_orders(created_at);
CREATE INDEX idx_target_orders_migrated_at ON target_orders(migrated_at);
CREATE INDEX idx_target_order_items_order_id ON target_order_items(order_id);
CREATE INDEX idx_target_order_items_product_name ON target_order_items(product_name);
CREATE INDEX idx_target_customer_events_customer_id ON target_customer_events(customer_id);
CREATE INDEX idx_target_customer_events_type ON target_customer_events(event_type);
CREATE INDEX idx_target_customer_events_timestamp ON target_customer_events(event_timestamp);
CREATE INDEX idx_target_customer_events_data ON target_customer_events USING GIN(event_data);
CREATE INDEX idx_target_inventory_product_id ON target_inventory_snapshots(product_id);
CREATE INDEX idx_target_inventory_timestamp ON target_inventory_snapshots(snapshot_timestamp);
CREATE INDEX idx_target_inventory_data ON target_inventory_snapshots USING GIN(snapshot_data);

-- Create migration tracking table
CREATE TABLE migration_status (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    total_records_migrated BIGINT DEFAULT 0,
    last_migrated_at TIMESTAMP,
    migration_status VARCHAR(50) DEFAULT 'in_progress',
    kafka_topic VARCHAR(255),
    last_kafka_offset BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_table, target_table)
);

INSERT INTO migration_status (source_table, target_table, kafka_topic) VALUES
    ('customers', 'target_customers', 'pg_local.public.customers'),
    ('orders', 'target_orders', 'pg_local.public.orders'),
    ('order_items', 'target_order_items', 'pg_local.public.order_items'),
    ('customer_events', 'target_customer_events', 'mongo_atlas.zdm_test.customer_events'),
    ('inventory_snapshots', 'target_inventory_snapshots', 'mongo_atlas.zdm_test.inventory_snapshots');

-- Create helper function
CREATE OR REPLACE FUNCTION update_migration_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE migration_status
    SET total_records_migrated = total_records_migrated + 1,
        last_migrated_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE target_table = TG_TABLE_NAME;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE TRIGGER track_customers_migration
    AFTER INSERT ON target_customers
    FOR EACH ROW EXECUTE FUNCTION update_migration_status();

CREATE TRIGGER track_orders_migration
    AFTER INSERT ON target_orders
    FOR EACH ROW EXECUTE FUNCTION update_migration_status();

CREATE TRIGGER track_order_items_migration
    AFTER INSERT ON target_order_items
    FOR EACH ROW EXECUTE FUNCTION update_migration_status();

CREATE TRIGGER track_customer_events_migration
    AFTER INSERT ON target_customer_events
    FOR EACH ROW EXECUTE FUNCTION update_migration_status();

CREATE TRIGGER track_inventory_migration
    AFTER INSERT ON target_inventory_snapshots
    FOR EACH ROW EXECUTE FUNCTION update_migration_status();

-- Verify
SELECT
    tablename as "Table Name",
    CASE
        WHEN tablename LIKE 'target_%' THEN 'Target table'
        WHEN tablename = 'migration_status' THEN 'Tracking table'
        ELSE 'Other'
    END as "Type"
FROM pg_tables
WHERE schemaname = 'public'
    AND (tablename LIKE 'target_%' OR tablename = 'migration_status')
ORDER BY tablename;

SELECT
    source_table as "Source",
    target_table as "Target",
    kafka_topic as "Kafka Topic",
    migration_status as "Status"
FROM migration_status
ORDER BY source_table;
