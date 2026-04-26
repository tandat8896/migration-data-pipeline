// ============================================================
// Lesson 2: Iceberg Basics - Create Database & Tables
// ============================================================

println("=" * 60)
println("LESSON 2: ICEBERG BASICS")
println("=" * 60)

// 1. Create database
println("\n1. Creating 'migration' database...")
spark.sql("CREATE DATABASE IF NOT EXISTS local.migration")
println("   ✓ Database created")

// 2. Show databases
println("\n2. List all databases:")
spark.sql("SHOW DATABASES IN local").show()

// 3. Create a simple Iceberg table
println("\n3. Creating Iceberg table 'test_users'...")

spark.sql("""
  CREATE TABLE IF NOT EXISTS local.migration.test_users (
    id BIGINT,
    name STRING,
    email STRING,
    created_at TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(created_at))
""")
println("   ✓ Table created")

// 4. Show tables in database
println("\n4. List tables in 'migration' database:")
spark.sql("SHOW TABLES IN local.migration").show()

// 5. Describe table
println("\n5. Table schema:")
spark.sql("DESCRIBE local.migration.test_users").show()

// 6. Insert test data
println("\n6. Inserting test data...")
import spark.implicits._
import java.sql.Timestamp

val testData = Seq(
  (1L, "Alice", "alice@example.com", Timestamp.valueOf("2026-04-25 10:00:00")),
  (2L, "Bob", "bob@example.com", Timestamp.valueOf("2026-04-25 11:00:00")),
  (3L, "Charlie", "charlie@example.com", Timestamp.valueOf("2026-04-25 12:00:00"))
).toDF("id", "name", "email", "created_at")

testData.writeTo("local.migration.test_users").append()
println("   ✓ 3 rows inserted")

// 7. Query data
println("\n7. Query data from Iceberg table:")
spark.sql("SELECT * FROM local.migration.test_users").show()

// 8. Count rows
println("\n8. Row count:")
val count = spark.sql("SELECT COUNT(*) as total FROM local.migration.test_users")
count.show()

// 9. Show table history (Iceberg feature!)
println("\n9. Table history (Iceberg time-travel):")
spark.sql("SELECT * FROM local.migration.test_users.history").show(truncate = false)

// 10. Show table snapshots
println("\n10. Table snapshots:")
spark.sql("SELECT * FROM local.migration.test_users.snapshots").show(truncate = false)

println("\n" + "=" * 60)
println("LESSON 2 COMPLETE!")
println("Key takeaway: Iceberg supports time-travel & snapshots!")
println("=" * 60)
