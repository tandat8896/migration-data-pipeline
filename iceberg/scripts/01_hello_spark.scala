// ============================================================
// Lesson 1: Hello Spark - Basics
// ============================================================

// Print separator
println("=" * 60)
println("LESSON 1: SPARK BASICS")
println("=" * 60)

// 1. Check Spark version
println("\n1. Spark Version:")
println(s"   Running Spark ${spark.version}")

// 2. Check Spark context
println("\n2. Spark Context:")
println(s"   App Name: ${spark.sparkContext.appName}")
println(s"   Master: ${spark.sparkContext.master}")

// 3. List available catalogs
println("\n3. Available Catalogs:")
spark.sql("SHOW CATALOGS").show()

// 4. List databases in 'local' catalog
println("\n4. Databases in 'local' catalog:")
spark.sql("SHOW DATABASES IN local").show()

// 5. Create a simple DataFrame (test data)
println("\n5. Create sample DataFrame:")
import spark.implicits._  // For .toDF() method

val data = Seq(
  ("Alice", 25, "Engineering"),
  ("Bob", 30, "Sales"),
  ("Charlie", 35, "Marketing")
)

val df = data.toDF("name", "age", "department")

println("   Sample data:")
df.show()

// 6. Basic DataFrame operations
println("\n6. DataFrame operations:")

println("   a) Schema:")
df.printSchema()

println("   b) Count rows:")
println(s"      Total rows: ${df.count()}")

println("   c) Filter (age > 25):")
df.filter($"age" > 25).show()

println("   d) Select specific columns:")
df.select("name", "department").show()

// 7. Register as temp view and use SQL
println("\n7. SQL queries:")
df.createOrReplaceTempView("employees")

spark.sql("SELECT department, COUNT(*) as count FROM employees GROUP BY department").show()

println("\n" + "=" * 60)
println("LESSON 1 COMPLETE!")
println("=" * 60)
