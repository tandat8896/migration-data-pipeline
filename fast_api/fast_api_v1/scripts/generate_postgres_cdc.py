#!/usr/bin/env python3
"""
Generate CDC test data for Postgres (Local or Supabase)
Usage:
    python generate_postgres_cdc.py --source local --rate 10 --duration 60
    python generate_postgres_cdc.py --source supabase --rate 5
"""

import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime
from typing import Optional

import psycopg2
from faker import Faker

fake = Faker()

# Global flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\nStopping data generation...")
    running = False


signal.signal(signal.SIGINT, signal_handler)


def get_connection(source: str):
    """Get database connection based on source."""
    if source == "local":
        # Read from env vars (set in fast_api/.env)
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            sslmode="require"
        )
    elif source == "supabase":
        # Use SUPABASE_DB_URL env var
        db_url = os.getenv("SUPABASE_DB_URL")
        if not db_url:
            raise ValueError("SUPABASE_DB_URL env var not set")
        conn = psycopg2.connect(db_url)
    else:
        raise ValueError(f"Unknown source: {source}")

    return conn


def get_random_customer_id(cursor) -> Optional[int]:
    """Get a random existing customer ID."""
    cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
    result = cursor.fetchone()
    return result[0] if result else None


def get_random_order_id(cursor) -> Optional[int]:
    """Get a random existing order ID."""
    cursor.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1")
    result = cursor.fetchone()
    return result[0] if result else None


def insert_customer(cursor) -> int:
    """INSERT a new customer."""
    email = fake.email()
    full_name = fake.name()
    phone = fake.phone_number()[:20]

    cursor.execute(
        """
        INSERT INTO customers (email, full_name, phone)
        VALUES (%s, %s, %s)
        RETURNING customer_id
        """,
        (email, full_name, phone)
    )
    customer_id = cursor.fetchone()[0]
    print(f"✅ INSERT customers | customer_id={customer_id} | email={email}")
    return customer_id


def insert_order(cursor) -> int:
    """INSERT a new order."""
    customer_id = get_random_customer_id(cursor)
    if not customer_id:
        # No customers yet, create one
        customer_id = insert_customer(cursor)

    total_amount = round(random.uniform(10.0, 500.0), 2)
    status = "pending"
    payment_method = random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"])

    cursor.execute(
        """
        INSERT INTO orders (customer_id, total_amount, status, payment_method)
        VALUES (%s, %s, %s, %s)
        RETURNING order_id
        """,
        (customer_id, total_amount, status, payment_method)
    )
    order_id = cursor.fetchone()[0]

    # Insert 1-3 order items
    num_items = random.randint(1, 3)
    for _ in range(num_items):
        product_name = fake.catch_phrase()
        quantity = random.randint(1, 5)
        price = round(random.uniform(5.0, 100.0), 2)

        cursor.execute(
            """
            INSERT INTO order_items (order_id, product_name, quantity, price)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_name, quantity, price)
        )

    print(f"✅ INSERT orders | order_id={order_id} | customer_id={customer_id} | total=${total_amount} | items={num_items}")
    return order_id


def update_customer(cursor) -> bool:
    """UPDATE a random customer."""
    customer_id = get_random_customer_id(cursor)
    if not customer_id:
        return False

    new_phone = fake.phone_number()[:20]
    cursor.execute(
        """
        UPDATE customers
        SET phone = %s, updated_at = NOW()
        WHERE customer_id = %s
        """,
        (new_phone, customer_id)
    )
    print(f"🔄 UPDATE customers | customer_id={customer_id} | phone={new_phone}")
    return True


def update_order_status(cursor) -> bool:
    """UPDATE a random order status."""
    order_id = get_random_order_id(cursor)
    if not order_id:
        return False

    # Get current status
    cursor.execute("SELECT status FROM orders WHERE order_id = %s", (order_id,))
    current_status = cursor.fetchone()[0]

    # Status progression
    status_flow = {
        "pending": ["confirmed", "cancelled"],
        "confirmed": ["shipped", "cancelled"],
        "shipped": ["delivered"],
        "delivered": [],
        "cancelled": []
    }

    next_statuses = status_flow.get(current_status, [])
    if not next_statuses:
        return False

    new_status = random.choice(next_statuses)
    cursor.execute(
        """
        UPDATE orders
        SET status = %s, updated_at = NOW()
        WHERE order_id = %s
        """,
        (new_status, order_id)
    )
    print(f"🔄 UPDATE orders | order_id={order_id} | status: {current_status} → {new_status}")
    return True


def delete_order(cursor) -> bool:
    """DELETE a cancelled order."""
    cursor.execute(
        """
        DELETE FROM orders
        WHERE order_id = (
            SELECT order_id FROM orders
            WHERE status = 'cancelled'
            ORDER BY RANDOM()
            LIMIT 1
        )
        RETURNING order_id
        """
    )
    result = cursor.fetchone()
    if result:
        order_id = result[0]
        print(f"❌ DELETE orders | order_id={order_id}")
        return True
    return False


def delete_customer(cursor) -> bool:
    """DELETE a customer with no orders."""
    cursor.execute(
        """
        DELETE FROM customers
        WHERE customer_id = (
            SELECT c.customer_id FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.order_id IS NULL
            ORDER BY RANDOM()
            LIMIT 1
        )
        RETURNING customer_id
        """
    )
    result = cursor.fetchone()
    if result:
        customer_id = result[0]
        print(f"❌ DELETE customers | customer_id={customer_id}")
        return True
    return False


def generate_operation(cursor):
    """Generate a random operation with weighted distribution."""
    rand = random.random()

    if rand < 0.60:  # 60% INSERT
        if random.random() < 0.3:
            insert_customer(cursor)
        else:
            insert_order(cursor)

    elif rand < 0.90:  # 30% UPDATE
        if random.random() < 0.2:
            if not update_customer(cursor):
                insert_customer(cursor)
        else:
            if not update_order_status(cursor):
                insert_order(cursor)

    else:  # 10% DELETE
        if random.random() < 0.5:
            if not delete_order(cursor):
                pass  # Skip if no cancelled orders
        else:
            if not delete_customer(cursor):
                pass  # Skip if no orphan customers


def main():
    parser = argparse.ArgumentParser(description="Generate CDC test data for Postgres")
    parser.add_argument("--source", choices=["local", "supabase"], required=True,
                        help="Target database: local or supabase")
    parser.add_argument("--rate", type=int, default=5,
                        help="Operations per second (default: 5)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Duration in seconds (0 = infinite, default: 0)")

    args = parser.parse_args()

    print(f"🚀 Starting CDC data generation")
    print(f"   Source: {args.source}")
    print(f"   Rate: {args.rate} ops/sec")
    print(f"   Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print(f"   Distribution: 60% INSERT, 30% UPDATE, 10% DELETE")
    print(f"   Press Ctrl+C to stop\n")

    conn = get_connection(args.source)
    conn.autocommit = True
    cursor = conn.cursor()

    start_time = time.time()
    operations = 0

    try:
        while running:
            # Check duration
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break

            try:
                generate_operation(cursor)
                operations += 1
            except Exception as e:
                print(f"⚠️  Error: {e}")

            # Rate limiting
            time.sleep(1.0 / args.rate)

    finally:
        cursor.close()
        conn.close()

        elapsed = time.time() - start_time
        print(f"\n📊 Summary:")
        print(f"   Total operations: {operations}")
        print(f"   Duration: {elapsed:.1f}s")
        print(f"   Avg rate: {operations/elapsed:.2f} ops/sec")


if __name__ == "__main__":
    main()
