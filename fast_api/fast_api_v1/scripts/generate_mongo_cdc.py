#!/usr/bin/env python3
"""
Generate CDC test data for MongoDB Atlas
Usage:
    python generate_mongo_cdc.py --rate 10 --duration 60
    python generate_mongo_cdc.py --rate 5
"""

import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

from pymongo import MongoClient
from faker import Faker

fake = Faker()

# Global flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n🛑 Stopping data generation...")
    running = False


signal.signal(signal.SIGINT, signal_handler)


def get_mongo_client():
    """Get MongoDB client from MONGO_URL env var."""
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise ValueError("MONGO_URL env var not set. Run: mongosh \"$MONGO_URL\" to verify.")

    client = MongoClient(mongo_url)
    return client


def get_random_document_id(collection, filter_dict=None):
    """Get a random document ID from collection."""
    pipeline = []
    if filter_dict:
        pipeline.append({"$match": filter_dict})
    pipeline.append({"$sample": {"size": 1}})

    result = list(collection.aggregate(pipeline))
    return result[0]["_id"] if result else None


def insert_customer_event(collection):
    """Insert a new customer event."""
    event = {
        "event_id": fake.uuid4(),
        "customer_id": random.randint(1000, 9999),
        "event_type": random.choice(["page_view", "add_to_cart", "purchase", "remove_from_cart"]),
        "timestamp": datetime.utcnow(),
        "metadata": {
            "page": fake.uri_path(),
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "session_id": fake.uuid4()[:12],
            "ip_address": fake.ipv4()
        }
    }

    result = collection.insert_one(event)
    print(f"✅ INSERT customer_events | _id={result.inserted_id} | type={event['event_type']} | customer={event['customer_id']}")
    return result.inserted_id


def insert_inventory_snapshot(collection):
    """Insert a new inventory snapshot."""
    snapshot = {
        "product_id": f"SKU-{random.randint(1000, 9999)}",
        "warehouse": random.choice(["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST"]),
        "quantity": random.randint(0, 500),
        "reorder_point": random.randint(10, 50),
        "unit_cost": round(random.uniform(5.0, 100.0), 2),
        "last_updated": datetime.utcnow(),
        "status": "active"
    }

    result = collection.insert_one(snapshot)
    print(f"✅ INSERT inventory_snapshots | _id={result.inserted_id} | product={snapshot['product_id']} | qty={snapshot['quantity']}")
    return result.inserted_id


def update_customer_event(collection) -> bool:
    """Update a random customer event (add metadata)."""
    doc_id = get_random_document_id(collection)
    if not doc_id:
        return False

    update = {
        "$set": {
            "metadata.updated_at": datetime.utcnow(),
            "metadata.processed": True
        }
    }

    result = collection.update_one({"_id": doc_id}, update)
    if result.modified_count > 0:
        print(f"🔄 UPDATE customer_events | _id={doc_id} | processed=True")
        return True
    return False


def update_inventory_quantity(collection) -> bool:
    """Update inventory quantity (simulate stock change)."""
    doc_id = get_random_document_id(collection, {"status": "active"})
    if not doc_id:
        return False

    # Random stock adjustment
    adjustment = random.randint(-50, 100)

    result = collection.update_one(
        {"_id": doc_id},
        {
            "$inc": {"quantity": adjustment},
            "$set": {"last_updated": datetime.utcnow()}
        }
    )

    if result.modified_count > 0:
        print(f"🔄 UPDATE inventory_snapshots | _id={doc_id} | qty_change={adjustment:+d}")
        return True
    return False


def delete_customer_event(collection) -> bool:
    """Delete an old processed event."""
    # Delete events older than 5 minutes that are processed
    old_time = datetime.utcnow() - timedelta(minutes=5)

    result = collection.delete_one({
        "timestamp": {"$lt": old_time},
        "metadata.processed": True
    })

    if result.deleted_count > 0:
        print(f"❌ DELETE customer_events | old processed event")
        return True
    return False


def delete_inventory_snapshot(collection) -> bool:
    """Delete inactive inventory records."""
    result = collection.delete_one({"status": "inactive"})

    if result.deleted_count > 0:
        print(f"❌ DELETE inventory_snapshots | inactive record")
        return True
    return False


def mark_inventory_inactive(collection) -> bool:
    """Mark a random inventory as inactive (before deleting)."""
    doc_id = get_random_document_id(collection, {"status": "active"})
    if not doc_id:
        return False

    result = collection.update_one(
        {"_id": doc_id},
        {"$set": {"status": "inactive", "last_updated": datetime.utcnow()}}
    )

    if result.modified_count > 0:
        print(f"🔄 UPDATE inventory_snapshots | _id={doc_id} | status=inactive")
        return True
    return False


def generate_operation(db):
    """Generate a random operation with weighted distribution."""
    rand = random.random()

    # Choose collection
    if random.random() < 0.7:
        collection = db.customer_events
        op_type = "events"
    else:
        collection = db.inventory_snapshots
        op_type = "inventory"

    if rand < 0.60:  # 60% INSERT
        if op_type == "events":
            insert_customer_event(collection)
        else:
            insert_inventory_snapshot(collection)

    elif rand < 0.90:  # 30% UPDATE
        if op_type == "events":
            if not update_customer_event(collection):
                insert_customer_event(collection)
        else:
            if not update_inventory_quantity(collection):
                insert_inventory_snapshot(collection)

    else:  # 10% DELETE
        if op_type == "events":
            if not delete_customer_event(collection):
                pass  # Skip if no old events
        else:
            # First mark as inactive, then delete
            if random.random() < 0.5:
                mark_inventory_inactive(collection)
            else:
                delete_inventory_snapshot(collection)


def main():
    parser = argparse.ArgumentParser(description="Generate CDC test data for MongoDB Atlas")
    parser.add_argument("--rate", type=int, default=5,
                        help="Operations per second (default: 5)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Duration in seconds (0 = infinite, default: 0)")
    parser.add_argument("--db", type=str, default="zdm_test",
                        help="Database name (default: zdm_test)")

    args = parser.parse_args()

    print(f"🚀 Starting MongoDB CDC data generation")
    print(f"   Database: {args.db}")
    print(f"   Collections: customer_events, inventory_snapshots")
    print(f"   Rate: {args.rate} ops/sec")
    print(f"   Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print(f"   Distribution: 60% INSERT, 30% UPDATE, 10% DELETE")
    print(f"   Press Ctrl+C to stop\n")

    client = get_mongo_client()
    db = client[args.db]

    # Ensure collections exist
    if "customer_events" not in db.list_collection_names():
        db.create_collection("customer_events")
    if "inventory_snapshots" not in db.list_collection_names():
        db.create_collection("inventory_snapshots")

    start_time = time.time()
    operations = 0

    try:
        while running:
            # Check duration
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break

            try:
                generate_operation(db)
                operations += 1
            except Exception as e:
                print(f"⚠️  Error: {e}")

            # Rate limiting
            time.sleep(1.0 / args.rate)

    finally:
        client.close()

        elapsed = time.time() - start_time
        print(f"\n📊 Summary:")
        print(f"   Total operations: {operations}")
        print(f"   Duration: {elapsed:.1f}s")
        print(f"   Avg rate: {operations/elapsed:.2f} ops/sec")


if __name__ == "__main__":
    main()
