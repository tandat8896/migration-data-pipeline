// ============================================================
// Lesson 3: Kafka Integration Basics
// ============================================================

println("=" * 60)
println("LESSON 3: KAFKA INTEGRATION")
println("=" * 60)

// IMPORTANT: This script demonstrates Kafka concepts
// Actual streaming requires running Kafka brokers

// 1. Kafka connection config
println("\n1. Kafka Configuration:")
val kafkaBootstrap = "localhost:9092"
val topic = "pg_local.public.customers"

println(s"   Bootstrap servers: $kafkaBootstrap")
println(s"   Topic: $topic")

// 2. Read from Kafka (batch mode - snapshot of current data)
println("\n2. Reading from Kafka (batch mode)...")
println("   NOTE: Kafka must be running for this to work!")

try {
  val kafkaDF = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()

  println(s"   ✓ Connected to Kafka topic: $topic")

  // 3. Show schema
  println("\n3. Kafka DataFrame schema:")
  kafkaDF.printSchema()

  // Kafka columns:
  // - key: binary (message key)
  // - value: binary (message payload - our CDC data!)
  // - topic: string
  // - partition: int
  // - offset: long
  // - timestamp: timestamp

  // 4. Count messages
  println("\n4. Message count:")
  val msgCount = kafkaDF.count()
  println(s"   Total messages in topic: $msgCount")

  // 5. Show raw messages (first 5)
  println("\n5. Raw Kafka messages (first 5):")
  kafkaDF.select("topic", "partition", "offset", "timestamp").show(5)

  // 6. Decode value as string
  println("\n6. Decoded message values:")
  import org.apache.spark.sql.functions._

  val decodedDF = kafkaDF.select(
    col("offset"),
    col("value").cast("string").as("json_value")
  )

  decodedDF.show(5, truncate = false)

  println("\n   ✓ Messages are in JSON format (Debezium CDC events)")

} catch {
  case e: Exception =>
    println(s"\n   ⚠ Warning: Could not connect to Kafka")
    println(s"   Error: ${e.getMessage}")
    println(s"\n   Make sure Kafka is running:")
    println(s"   cd ~/tandat_project/kafka-data && nix develop")
}

println("\n" + "=" * 60)
println("LESSON 3 COMPLETE!")
println("Next: Parse Debezium JSON and write to Iceberg")
println("=" * 60)
