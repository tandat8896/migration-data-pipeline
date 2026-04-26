// ============================================================
// Lesson 5: Upsert to Supabase Target
// Pipeline: Iceberg Bronze → Supabase Target Database
// ============================================================

println("=" * 60)
println("LESSON 5: UPSERT TO SUPABASE TARGET")
println("=" * 60)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// Step 1: Read from Iceberg Bronze
println("\nStep 1: Reading from Iceberg Bronze...")
val bronzeDF = spark.sql("SELECT * FROM local.migration.cdc_bronze")
val bronzeCount = bronzeDF.count()
println(s"   Records in Bronze: $bronzeCount")

// Step 2: Show Bronze schema
println("\nStep 2: Bronze table schema:")
bronzeDF.printSchema()

// Step 3: Transform to target schema
println("\nStep 3: Transforming to target schema...")
val targetDF = bronzeDF.select(col("customer_id"), col("email"), col("full_name"), col("phone"), from_unixtime(col("created_at") / 1000000).cast("timestamp").alias("created_at"), from_unixtime(col("updated_at") / 1000000).cast("timestamp").alias("updated_at"), lit("postgres_local").alias("source_system"), current_timestamp().alias("migrated_at"), col("lsn").cast("string").alias("cdc_lsn"))

println("   Target schema:")
targetDF.printSchema()

// Step 4: Show sample data
println("\nStep 4: Sample target data:")
targetDF.select("customer_id", "email", "full_name", "source_system").show(5, false)

// Step 5: Setup JDBC connection
println("\nStep 5: Setting up Supabase connection...")
val rawUrl = sys.env.getOrElse("SUPABASE_DB_URL", "")

if (rawUrl.isEmpty) {
  println("   ⚠ ERROR: SUPABASE_DB_URL not set!")
  println("   Set environment variable:")
  println("   export SUPABASE_DB_URL='postgresql://user.project:pass@host:5432/postgres'")
} else {
  val urlPattern = "postgresql://([^:]+):([^@]+)@(.+)".r
  val urlPattern(jdbcUser, jdbcPassword, hostPart) = rawUrl
  val jdbcUrl = s"jdbc:postgresql://${hostPart.split("\\?")(0)}"

  println(s"   JDBC URL: ${jdbcUrl}")
  println(s"   User: ${jdbcUser}")
  println("   Password: ****")

  // Step 6: Write to Supabase (Append mode for now)
  println("\nStep 6: Writing to Supabase target_customers...")
  println("   Mode: APPEND (will add duplicates if run multiple times)")
  println("   Writing...")

  targetDF.write.format("jdbc").option("url", jdbcUrl).option("dbtable", "target_customers").option("user", jdbcUser).option("password", jdbcPassword).option("driver", "org.postgresql.Driver").option("batchsize", "100").mode("append").save()

  println("   ✓ Write complete!")

  // Step 7: Verify data in Supabase
  println("\nStep 7: Verify data in Supabase...")
  val verifyDF = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "target_customers").option("user", jdbcUser).option("password", jdbcPassword).option("driver", "org.postgresql.Driver").load()

  val targetCount = verifyDF.count()
  println(s"   Records in target_customers: $targetCount")

  println("\n   Sample from target:")
  verifyDF.select("customer_id", "email", "full_name", "source_system", "migrated_at").show(5, false)

  // Step 8: Check migration_status table
  println("\nStep 8: Check migration_status...")
  val statusDF = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "migration_status").option("user", jdbcUser).option("password", jdbcPassword).option("driver", "org.postgresql.Driver").load()

  statusDF.filter(col("source_table") === "customers").select("source_table", "target_table", "total_records_migrated", "last_migrated_at", "migration_status").show(false)

  println("\n   NOTE: migration_status updated by trigger on INSERT")
}

println("\n" + "=" * 60)
println("LESSON 5 COMPLETE!")
println(s"✓ Pipeline: Iceberg Bronze → Supabase Target")
println("✓ Next: Implement UPSERT logic to handle updates")
println("=" * 60)
