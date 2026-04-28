// ============================================================================
// LESSON 10: Read Confluent Avro from Kafka v4 → Write to Iceberg
// ============================================================================
// Topic: pg_local_avro_v4.public.customers
// Schema Registry: http://localhost:8081
// Output: Iceberg Bronze table
// ============================================================================

println("=" * 80)
println("LESSON 10: Confluent Avro (v4) → Iceberg Bronze")
println("=" * 80)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Config
val schemaRegistryUrl = "http://localhost:8081"
val kafkaServers = "localhost:9092"
val topicPrefix = "pg_local_avro_v4.public"

// ============================================================================
// STEP 1: Read raw bytes from Kafka
// ============================================================================
println("\nSTEP 1: Read raw Kafka messages")
println("-" * 80)

// Read Kafka - single line to avoid REPL bug
val rawKafkaDF = spark.read.format("kafka").option("kafka.bootstrap.servers", kafkaServers).option("subscribe", s"$topicPrefix.customers").option("startingOffsets", "earliest").load()

println(s"Total messages: ${rawKafkaDF.count()}")

// ============================================================================
// STEP 2: Parse Confluent wire format (5-byte header)
// ============================================================================
println("\nSTEP 2: Extract Schema ID from Confluent wire format")
println("-" * 80)

// Parse wire format - single statement to avoid REPL bug
val withSchemaId = rawKafkaDF.withColumn("magic_byte", substring(col("value"), 1, 1)).withColumn("schema_id_bytes", substring(col("value"), 2, 4)).withColumn("schema_id", conv(hex(col("schema_id_bytes")), 16, 10).cast("int")).withColumn("avro_data", substring(col("value"), 6, 100000))

// Show schema IDs
withSchemaId.select("schema_id", "magic_byte").show(5, false)

// ============================================================================
// STEP 3: Get unique schema IDs
// ============================================================================
println("\nSTEP 3: Get unique schema IDs")
println("-" * 80)

val schemaIds = withSchemaId.select("schema_id").distinct().collect().map(_.getInt(0))
println(s"Unique schema IDs: ${schemaIds.mkString(", ")}")

// Pick latest schema ID
val latestSchemaId = schemaIds.max
println(s"Using schema ID: $latestSchemaId")

// ============================================================================
// STEP 4: Fetch Avro schema from Confluent Registry
// ============================================================================
println("\nSTEP 4: Fetch schema from Confluent Registry")
println("-" * 80)

import sys.process._
import com.fasterxml.jackson.databind.ObjectMapper

val mapper = new ObjectMapper()
val schemaResponse = s"curl -s $schemaRegistryUrl/schemas/ids/$latestSchemaId".!!
val schemaJson = mapper.readTree(schemaResponse)
val schemaString = schemaJson.get("schema").asText()

println(s"Schema fetched (length: ${schemaString.length} chars)")
println(s"Schema preview: ${schemaString.take(200)}...")

// ============================================================================
// STEP 5: Deserialize Avro using from_avro
// ============================================================================
println("\nSTEP 5: Deserialize Avro to struct")
println("-" * 80)

import org.apache.spark.sql.avro.functions.from_avro

// Filter to only use latest schema ID
val cleanKafkaDF = withSchemaId.filter(col("schema_id") === latestSchemaId)

// Deserialize - single line
val deserializedDF = cleanKafkaDF.select(col("topic"), col("partition"), col("offset"), col("timestamp").alias("kafka_timestamp"), from_avro(col("avro_data"), schemaString).alias("cdc_event"))

println("Deserialized schema:")
deserializedDF.printSchema()

deserializedDF.show(5, false)

// ============================================================================
// STEP 6: Flatten CDC envelope
// ============================================================================
println("\nSTEP 6: Flatten CDC fields")
println("-" * 80)

// Flatten CDC envelope
val flattenedDF = deserializedDF.select(col("topic"), col("partition"), col("offset"), col("kafka_timestamp"), col("cdc_event.before").alias("before"), col("cdc_event.after").alias("after"), col("cdc_event.op").alias("operation"), col("cdc_event.ts_ms").alias("cdc_timestamp_ms"))

flattenedDF.printSchema()
flattenedDF.show(5, false)

// ============================================================================
// STEP 7: Write to Iceberg Bronze
// ============================================================================
println("\nSTEP 7: Write to Iceberg Bronze table")
println("-" * 80)

// Add metadata columns
val bronzeDF = flattenedDF.withColumn("ingestion_time", current_timestamp()).withColumn("source_topic", col("topic"))

// Create database
spark.sql("CREATE DATABASE IF NOT EXISTS local.migration")

// Write to Iceberg - single line
bronzeDF.writeTo("local.migration.cdc_bronze_v4").tableProperty("format-version", "2").partitionedBy(days(col("kafka_timestamp"))).createOrReplace()

println("✅ Written to Iceberg: local.migration.cdc_bronze_v4")

// ============================================================================
// STEP 8: Verify Iceberg table
// ============================================================================
println("\nSTEP 8: Verify Iceberg table")
println("-" * 80)

val verifyDF = spark.table("local.migration.cdc_bronze_v4")
println(s"Total records in Iceberg: ${verifyDF.count()}")

verifyDF.select("operation", "source_topic").groupBy("operation", "source_topic").count().show()

println("\nSample records:")
verifyDF.select("operation", "after.customer_id", "after.full_name", "after.email")
  .show(10, false)

println("\n" + "=" * 80)
println("✅ LESSON 10 COMPLETE: Confluent Avro v4 → Iceberg Bronze")
println("=" * 80)
println("\nNext steps:")
println("1. Check Iceberg table: spark.table(\"local.migration.cdc_bronze_v4\").show()")
println("2. Write upsert logic to Supabase target tables")
println("3. Implement CDC streaming job for real-time updates")
