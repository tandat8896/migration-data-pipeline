// ============================================================
// Lesson 6: MongoDB CDC Consumer
// Read MongoDB CDC events from Kafka → Iceberg Bronze → Supabase
// ============================================================

println("=" * 60)
println("LESSON 6: MONGODB CDC CONSUMER")
println("=" * 60)

// Import libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import spark.implicits._

// ============================================================
// PART 1: CUSTOMER EVENTS PIPELINE
// ============================================================

println("\n" + "=" * 60)
println("PART 1: CUSTOMER EVENTS")
println("=" * 60)

// Step 1: Read customer_events from Kafka
println("\nStep 1: Reading customer_events from Kafka...")
val eventsKafkaDF = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "mongo_atlas.zdm_test.customer_events").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
val eventsCount = eventsKafkaDF.count()
println(s"   Messages read: $eventsCount")

// Step 2: Define MongoDB Debezium schema for customer_events
println("\nStep 2: Defining MongoDB CDC schema...")
val mongoEventSchema = new StructType().add("payload", new StructType().add("op", StringType).add("ts_ms", LongType).add("source", new StructType().add("connector", StringType).add("name", StringType).add("db", StringType).add("collection", StringType)).add("after", StringType))
println("   Schema defined")

// Step 3: Parse JSON (MongoDB stores 'after' as JSON string)
println("\nStep 3: Parsing MongoDB CDC JSON...")
val eventsDecodedDF = eventsKafkaDF.select(col("offset"), col("timestamp").alias("kafka_timestamp"), from_json(col("value").cast("string"), mongoEventSchema).alias("data"))
println("   JSON decoded")

// Step 4: Define schema for the 'after' field (customer_events document)
println("\nStep 4: Defining customer_events document schema...")
val customerEventDocSchema = new StructType().add("_id", StringType).add("event_id", StringType).add("customer_id", IntegerType).add("event_type", StringType).add("timestamp", StringType).add("metadata", new StructType().add("page", StringType).add("device", StringType).add("session_id", StringType).add("ip_address", StringType).add("updated_at", StringType).add("processed", BooleanType))
println("   Document schema defined")

// Step 5: Parse the 'after' field as nested JSON
println("\nStep 5: Parsing MongoDB document...")
val eventsParsedDF = eventsDecodedDF.select(col("offset"), col("kafka_timestamp"), col("data.payload.op").alias("operation"), col("data.payload.ts_ms").alias("event_timestamp"), col("data.payload.source.collection").alias("source_collection"), from_json(col("data.payload.after"), customerEventDocSchema).alias("doc"))
println("   Document parsed")

// Step 6: Flatten the document structure
println("\nStep 6: Flattening document structure...")
val eventsFlatDF = eventsParsedDF.select(col("offset"), col("kafka_timestamp"), col("operation"), col("event_timestamp"), col("source_collection"), col("doc._id").alias("mongo_id"), col("doc.event_id"), col("doc.customer_id"), col("doc.event_type"), col("doc.timestamp").alias("event_occurred_at"), col("doc.metadata.page").alias("page"), col("doc.metadata.device").alias("device"), col("doc.metadata.session_id").alias("session_id"), col("doc.metadata.ip_address").alias("ip_address"))
val eventsFlatCount = eventsFlatDF.count()
println(s"   Flattened records: $eventsFlatCount")

// Step 7: Show sample
println("\nStep 7: Sample customer_events data:")
eventsFlatDF.select("operation", "customer_id", "event_type", "device").show(10, false)

// Step 8: Add ingestion timestamp
println("\nStep 8: Adding ingestion timestamp...")
val eventsBronzeDF = eventsFlatDF.withColumn("ingested_at", current_timestamp())
println("   Timestamp added")

// Step 9: Write to Iceberg Bronze (reuse cdc_bronze table)
println("\nStep 9: Writing customer_events to Iceberg Bronze...")
eventsBronzeDF.createOrReplaceTempView("temp_events_bronze")
spark.sql("""
  INSERT INTO local.migration.cdc_bronze
  SELECT
    offset,
    kafka_timestamp,
    operation,
    event_timestamp,
    source_collection as source_table,
    0 as lsn,
    customer_id,
    event_type as email,
    event_id as full_name,
    device as phone,
    CAST(unix_timestamp(event_occurred_at, "yyyy-MM-dd'T'HH:mm:ss") AS BIGINT) as created_at,
    0 as updated_at,
    ingested_at
  FROM temp_events_bronze
""")
val totalBronze = spark.sql("SELECT COUNT(*) as cnt FROM local.migration.cdc_bronze").first().getLong(0)
println(s"   Total records in Bronze: $totalBronze")

// ============================================================
// PART 2: INVENTORY SNAPSHOTS PIPELINE
// ============================================================

println("\n" + "=" * 60)
println("PART 2: INVENTORY SNAPSHOTS")
println("=" * 60)

// Step 10: Read inventory_snapshots from Kafka
println("\nStep 10: Reading inventory_snapshots from Kafka...")
val inventoryKafkaDF = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "mongo_atlas.zdm_test.inventory_snapshots").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
val inventoryCount = inventoryKafkaDF.count()
println(s"   Messages read: $inventoryCount")

// Step 11: Parse inventory JSON
println("\nStep 11: Parsing inventory CDC JSON...")
val inventoryDecodedDF = inventoryKafkaDF.select(col("offset"), col("timestamp").alias("kafka_timestamp"), from_json(col("value").cast("string"), mongoEventSchema).alias("data"))
println("   JSON decoded")

// Step 12: Define inventory document schema
println("\nStep 12: Defining inventory document schema...")
val inventoryDocSchema = new StructType().add("_id", StringType).add("product_id", StringType).add("warehouse", StringType).add("quantity", IntegerType).add("reorder_point", IntegerType).add("unit_cost", DoubleType).add("last_updated", StringType).add("status", StringType)
println("   Document schema defined")

// Step 13: Parse inventory document
println("\nStep 13: Parsing inventory document...")
val inventoryParsedDF = inventoryDecodedDF.select(col("offset"), col("kafka_timestamp"), col("data.payload.op").alias("operation"), col("data.payload.ts_ms").alias("event_timestamp"), col("data.payload.source.collection").alias("source_collection"), from_json(col("data.payload.after"), inventoryDocSchema).alias("doc"))
println("   Document parsed")

// Step 14: Flatten inventory structure
println("\nStep 14: Flattening inventory structure...")
val inventoryFlatDF = inventoryParsedDF.select(col("offset"), col("kafka_timestamp"), col("operation"), col("event_timestamp"), col("source_collection"), col("doc._id").alias("mongo_id"), col("doc.product_id"), col("doc.warehouse"), col("doc.quantity"), col("doc.reorder_point"), col("doc.unit_cost"), col("doc.last_updated"), col("doc.status"))
val inventoryFlatCount = inventoryFlatDF.count()
println(s"   Flattened records: $inventoryFlatCount")

// Step 15: Show sample
println("\nStep 15: Sample inventory data:")
inventoryFlatDF.select("operation", "product_id", "warehouse", "quantity", "status").show(10, false)

// ============================================================
// PART 3: WRITE TO SUPABASE
// ============================================================

println("\n" + "=" * 60)
println("PART 3: UPSERT TO SUPABASE")
println("=" * 60)

// Step 16: Load Supabase credentials
println("\nStep 16: Loading Supabase credentials...")
val supabaseUrl = sys.env.getOrElse("SUPABASE_DB_URL", "")
if (supabaseUrl.isEmpty) { println("   ERROR: SUPABASE_DB_URL not set!"); sys.exit(1) }
val urlPattern = "postgresql://([^:]+):([^@]+)@(.+)".r
val urlPattern(user, pass, hostDb) = supabaseUrl
val jdbcUrl = s"jdbc:postgresql://$hostDb"
println(s"   User: ${user.split('.')(0)}...")
println(s"   Host: ${hostDb.split('/')(0)}")

// Step 17: Prepare customer_events for Supabase
println("\nStep 17: Preparing customer_events for Supabase...")
val eventsWithJson = eventsFlatDF.filter(col("operation") === "r" || col("operation") === "c").withColumn("event_data", to_json(struct(col("page"), col("device"), col("session_id"), col("ip_address")))).withColumn("event_timestamp_parsed", to_timestamp(col("event_occurred_at"))).withColumn("source_system", lit("mongodb_atlas")).withColumn("migrated_at", current_timestamp()).withColumn("event_type_mapped", when(col("event_type") === "page_view", "view").when(col("event_type") === "add_to_cart", "cart_add").when(col("event_type") === "remove_from_cart", "cart_remove").otherwise(col("event_type")))
val eventsForSupabase = eventsWithJson.select(col("event_id"), col("customer_id"), col("event_type_mapped").alias("event_type"), col("event_data"), col("event_timestamp_parsed").alias("event_timestamp"), col("source_system"), col("migrated_at"), col("mongo_id").alias("mongo_object_id"))
val eventsSupabaseCount = eventsForSupabase.count()
println(s"   Records to migrate: $eventsSupabaseCount")

// Step 18: Write customer_events to Supabase (via temp table + SQL cast)
println("\nStep 18: Writing customer_events to Supabase...")
val eventsTempTable = "temp_customer_events_" + System.currentTimeMillis()
eventsForSupabase.write.format("jdbc").option("url", jdbcUrl).option("dbtable", eventsTempTable).option("user", user).option("password", pass).option("driver", "org.postgresql.Driver").mode(SaveMode.Overwrite).save()
val conn = java.sql.DriverManager.getConnection(jdbcUrl, user, pass)
val stmt = conn.createStatement()
stmt.execute(s"""
  INSERT INTO target_customer_events (event_id, customer_id, event_type, event_data, event_timestamp, source_system, migrated_at, mongo_object_id)
  SELECT event_id, customer_id, event_type, event_data::jsonb, event_timestamp, source_system, migrated_at, mongo_object_id
  FROM $eventsTempTable
  ON CONFLICT (event_id) DO NOTHING
""")
val insertedRows = stmt.getUpdateCount()
stmt.execute(s"DROP TABLE IF EXISTS $eventsTempTable")
stmt.close()
conn.close()
println(s"   ✓ Customer events migrated ($insertedRows rows)")

// Step 19: Prepare inventory for Supabase
println("\nStep 19: Preparing inventory for Supabase...")
val inventoryWithJson = inventoryFlatDF.filter(col("operation") === "r" || col("operation") === "c").withColumn("snapshot_data", to_json(struct(col("reorder_point"), col("unit_cost"), col("status")))).withColumn("snapshot_timestamp_parsed", to_timestamp(col("last_updated"))).withColumn("source_system", lit("mongodb_atlas")).withColumn("migrated_at", current_timestamp()).withColumn("quantity_fixed", when(col("quantity") < 0, 0).otherwise(col("quantity")))
val inventoryForSupabase = inventoryWithJson.select(col("mongo_id").alias("snapshot_id"), col("product_id"), col("quantity_fixed").alias("quantity"), col("warehouse").alias("warehouse_location"), col("snapshot_data"), col("snapshot_timestamp_parsed").alias("snapshot_timestamp"), col("source_system"), col("migrated_at"), col("mongo_id").alias("mongo_object_id"))
val inventorySupabaseCount = inventoryForSupabase.count()
println(s"   Records to migrate: $inventorySupabaseCount")

// Step 20: Write inventory to Supabase (via temp table + SQL cast)
println("\nStep 20: Writing inventory to Supabase...")
val inventoryTempTable = "temp_inventory_snapshots_" + System.currentTimeMillis()
inventoryForSupabase.write.format("jdbc").option("url", jdbcUrl).option("dbtable", inventoryTempTable).option("user", user).option("password", pass).option("driver", "org.postgresql.Driver").mode(SaveMode.Overwrite).save()
val conn2 = java.sql.DriverManager.getConnection(jdbcUrl, user, pass)
val stmt2 = conn2.createStatement()
stmt2.execute(s"""
  INSERT INTO target_inventory_snapshots (snapshot_id, product_id, quantity, warehouse_location, snapshot_data, snapshot_timestamp, source_system, migrated_at, mongo_object_id)
  SELECT snapshot_id, product_id, quantity, warehouse_location, snapshot_data::jsonb, snapshot_timestamp, source_system, migrated_at, mongo_object_id
  FROM $inventoryTempTable
  ON CONFLICT (snapshot_id) DO NOTHING
""")
val insertedRows2 = stmt2.getUpdateCount()
stmt2.execute(s"DROP TABLE IF EXISTS $inventoryTempTable")
stmt2.close()
conn2.close()
println(s"   ✓ Inventory snapshots migrated ($insertedRows2 rows)")

// ============================================================
// FINAL SUMMARY
// ============================================================

println("\n" + "=" * 60)
println("LESSON 6 COMPLETE!")
println("=" * 60)
println(s"✓ Customer events: $insertedRows migrated")
println(s"✓ Inventory snapshots: $insertedRows2 migrated")
println(s"✓ Total MongoDB records: ${insertedRows + insertedRows2}")
println(s"✓ Iceberg Bronze total: $totalBronze records")
println("✓ Pipeline: MongoDB → Kafka → Spark → Iceberg + Supabase")
println("=" * 60)
