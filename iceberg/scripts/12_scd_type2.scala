// ============================================================================
// LESSON 12: SCD Type 2 - Slowly Changing Dimension
// ============================================================================
// Input: Iceberg Bronze (CDC events)
// Output: Iceberg Silver (SCD Type 2 with history tracking)
// ============================================================================

println("=" * 80)
println("LESSON 12: SCD Type 2 - History Tracking")
println("=" * 80)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// ============================================================================
// STEP 1: Read CDC Bronze data
// ============================================================================
println("\nSTEP 1: Read CDC events from Bronze")
println("-" * 80)

val bronzeDF = spark.table("local.migration.cdc_bronze_v4")

println(s"Total CDC events: ${bronzeDF.count()}")
bronzeDF.groupBy("operation").count().show()

// ============================================================================
// STEP 2: Extract customer data from 'after' field
// ============================================================================
println("\nSTEP 2: Extract customer attributes")
println("-" * 80)

val customersDF = bronzeDF
  .filter(col("operation") === "r" || col("operation") === "u" || col("operation") === "c")
  .select(
    col("after.customer_id").alias("customer_id"),
    col("after.full_name").alias("full_name"),
    col("after.email").alias("email"),
    col("after.phone").alias("phone"),
    col("after.created_at").alias("created_at"),
    col("after.updated_at").alias("updated_at"),
    col("cdc_timestamp_ms").alias("event_timestamp_ms"),
    col("operation")
  )
  .withColumn("event_time", from_unixtime(col("event_timestamp_ms") / 1000).cast("timestamp"))

customersDF.show(10, false)

// ============================================================================
// STEP 3: Add SCD Type 2 columns
// ============================================================================
println("\nSTEP 3: Add SCD Type 2 metadata")
println("-" * 80)

// Window to get latest event per customer
val windowSpec = Window.partitionBy("customer_id").orderBy(col("event_timestamp_ms").desc)

val scdDF = customersDF
  .withColumn("rank", row_number().over(windowSpec))
  .withColumn("is_current", when(col("rank") === 1, true).otherwise(false))
  .withColumn("valid_from", col("event_time"))
  .withColumn("valid_to", when(col("is_current"), lit(null).cast("timestamp")).otherwise(col("event_time")))
  .withColumn("version", col("rank"))
  .drop("rank", "event_timestamp_ms", "operation")

println("SCD Type 2 schema:")
scdDF.printSchema()

scdDF.filter(col("is_current") === true).show(10, false)

// ============================================================================
// STEP 4: Write to Iceberg Silver (SCD Type 2)
// ============================================================================
println("\nSTEP 4: Write to Iceberg Silver table")
println("-" * 80)

spark.sql("CREATE DATABASE IF NOT EXISTS local.migration")

scdDF.writeTo("local.migration.customers_scd2")
  .tableProperty("format-version", "2")
  .partitionedBy(col("is_current"))
  .createOrReplace()

println("✅ Written to Iceberg: local.migration.customers_scd2")

// ============================================================================
// STEP 5: Verify SCD Type 2 table
// ============================================================================
println("\nSTEP 5: Verify SCD Type 2 table")
println("-" * 80)

val scd2Table = spark.table("local.migration.customers_scd2")

println(s"Total records: ${scd2Table.count()}")
println("\nCurrent vs Historical:")
scd2Table.groupBy("is_current").count().show()

println("\nSample current records:")
scd2Table.filter(col("is_current") === true)
  .select("customer_id", "full_name", "email", "valid_from", "is_current", "version")
  .show(10, false)

// ============================================================================
// STEP 6: Query historical changes (if any)
// ============================================================================
println("\nSTEP 6: Check for customers with history")
println("-" * 80)

val customersWithHistory = scd2Table
  .groupBy("customer_id")
  .agg(count("*").alias("version_count"))
  .filter(col("version_count") > 1)

if (customersWithHistory.count() > 0) {
  println("Customers with multiple versions:")
  customersWithHistory.show()

  val sampleCustomerId = customersWithHistory.first().getInt(0)
  println(s"\nHistory for customer_id = $sampleCustomerId:")
  scd2Table.filter(col("customer_id") === sampleCustomerId)
    .orderBy(col("version").desc)
    .select("customer_id", "full_name", "email", "valid_from", "valid_to", "is_current", "version")
    .show(false)
} else {
  println("No customers with history yet (all snapshot data)")
  println("Insert/Update some records in Postgres to see SCD Type 2 in action!")
}

// ============================================================================
// STEP 7: Demonstrate time-travel query
// ============================================================================
println("\nSTEP 7: Time-travel query example")
println("-" * 80)

println("Query: Get current records (as of now):")
scd2Table.filter(col("is_current") === true)
  .select("customer_id", "full_name", "email")
  .show(5, false)

println("\nQuery: Get all versions of a customer:")
val sampleId = scd2Table.first().getInt(0)
scd2Table.filter(col("customer_id") === sampleId)
  .orderBy(col("version").desc)
  .select("customer_id", "full_name", "email", "valid_from", "valid_to", "version")
  .show(false)

println("\n" + "=" * 80)
println("✅ LESSON 12 COMPLETE: SCD Type 2 History Tracking")
println("=" * 80)

println("\nNext steps to test SCD Type 2:")
println("1. Update a customer in Postgres:")
println("   UPDATE customers SET full_name = 'New Name' WHERE customer_id = 1;")
println("2. Wait for Debezium to capture the change")
println("3. Re-run this script to see version history!")
println("\nQueries:")
println("  Current: spark.table(\"local.migration.customers_scd2\").filter(col(\"is_current\") === true).show()")
println("  History: spark.table(\"local.migration.customers_scd2\").filter(col(\"customer_id\") === 1).show()")
