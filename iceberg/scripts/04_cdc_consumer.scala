// ============================================================
// Lesson 4: CDC Consumer - Working Version
// All statements on single lines for Scala shell compatibility
// ============================================================

println("=" * 60)
println("LESSON 4: CDC CONSUMER - Kafka to Iceberg")
println("=" * 60)

// Import libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// Step 1: Read from Kafka
println("\nStep 1: Reading from Kafka...")
val kafkaDF = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "pg_local.public.customers").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
val msgCount = kafkaDF.count()
println(s"   Messages read: $msgCount")

// Step 2: Define Debezium schema (with payload wrapper)
println("\nStep 2: Defining Debezium schema...")
val debeziumSchema = new StructType().add("payload", new StructType().add("op", StringType).add("ts_ms", LongType).add("source", new StructType().add("version", StringType).add("connector", StringType).add("name", StringType).add("ts_ms", LongType).add("db", StringType).add("schema", StringType).add("table", StringType).add("lsn", LongType)).add("before", new StructType().add("customer_id", IntegerType).add("email", StringType).add("full_name", StringType).add("phone", StringType).add("created_at", LongType).add("updated_at", LongType)).add("after", new StructType().add("customer_id", IntegerType).add("email", StringType).add("full_name", StringType).add("phone", StringType).add("created_at", LongType).add("updated_at", LongType)))
println("   Schema defined")

// Step 3: Parse JSON
println("\nStep 3: Parsing Debezium JSON...")
val decodedDF = kafkaDF.select(col("offset"), col("timestamp").alias("kafka_timestamp"), from_json(col("value").cast("string"), debeziumSchema).alias("data"))
println("   JSON decoded")

// Step 4: Flatten structure
println("\nStep 4: Flattening CDC events...")
val parsedDF = decodedDF.select(col("offset"), col("kafka_timestamp"), col("data.payload.op").alias("operation"), col("data.payload.ts_ms").alias("event_timestamp"), col("data.payload.source.table").alias("source_table"), col("data.payload.source.lsn").alias("lsn"), col("data.payload.after.customer_id").alias("customer_id"), col("data.payload.after.email").alias("email"), col("data.payload.after.full_name").alias("full_name"), col("data.payload.after.phone").alias("phone"), col("data.payload.after.created_at").alias("created_at"), col("data.payload.after.updated_at").alias("updated_at"))
val parsedCount = parsedDF.count()
println(s"   Parsed records: $parsedCount")

// Step 5: Show sample
println("\nStep 5: Sample parsed data:")
parsedDF.select("operation", "customer_id", "email", "full_name").show(10, false)

// Step 6: Operation summary
println("\nStep 6: Operation summary:")
parsedDF.groupBy("operation").count().show()

// Step 7: Create Iceberg Bronze table
println("\nStep 7: Creating Iceberg Bronze table...")
spark.sql("DROP TABLE IF EXISTS local.migration.cdc_bronze")
spark.sql("CREATE TABLE local.migration.cdc_bronze (offset BIGINT, kafka_timestamp TIMESTAMP, operation STRING, event_timestamp BIGINT, source_table STRING, lsn BIGINT, customer_id INT, email STRING, full_name STRING, phone STRING, created_at BIGINT, updated_at BIGINT, ingested_at TIMESTAMP) USING iceberg PARTITIONED BY (days(kafka_timestamp))")
println("   Table created")

// Step 8: Add ingestion timestamp
println("\nStep 8: Adding ingestion timestamp...")
val bronzeDF = parsedDF.withColumn("ingested_at", current_timestamp())
println("   Timestamp added")

// Step 9: Write to Iceberg Bronze
println("\nStep 9: Writing to Iceberg Bronze...")
bronzeDF.writeTo("local.migration.cdc_bronze").append()
val bronzeCount = spark.sql("SELECT COUNT(*) as cnt FROM local.migration.cdc_bronze").first().getLong(0)
println(s"   Records in Bronze: $bronzeCount")

// Step 10: Verify Bronze table
println("\nStep 10: Verify Bronze table:")
spark.sql("SELECT operation, customer_id, email, full_name FROM local.migration.cdc_bronze LIMIT 10").show(false)

// Step 11: Final summary
println("\nStep 11: Final CDC Summary:")
spark.sql("SELECT operation, COUNT(*) as count FROM local.migration.cdc_bronze GROUP BY operation ORDER BY operation").show()

println("\n" + "=" * 60)
println("LESSON 4 COMPLETE!")
println(s"✓ Processed $parsedCount CDC events")
println(s"✓ Written $bronzeCount records to Iceberg Bronze")
println("✓ Pipeline: Kafka → Iceberg Bronze")
println("=" * 60)
