import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import java.io.File
import org.orekit.data.DataContext
import org.orekit.data.DirectoryCrawler
import org.orekit.propagation.analytical.tle.TLE
import org.orekit.propagation.analytical.tle.TLEPropagator
import org.orekit.frames.FramesFactory

/**
 * TLE Stream Processor - Scala Spark Structured Streaming
 *
 * Mirrors the reference project's spark_sgp4_to_hdfs.py pipeline in Scala/Orekit.
 *
 * Flow:
 *   Kafka (tle-raw)
 *     → Spark Structured Streaming
 *     → Orekit SGP4 UDF  (position/velocity vectors in ECI km)
 *     → Tracking stop conditions  (low altitude, SGP4 error, stale TLE)
 *     → HDFS Parquet (active vectors, partitioned by date)
 *     → HDFS Parquet (stopped-tracking log)
 *     → Console sink (live monitoring)
 *
 * Run:
 *   sbt "runMain TLEStreamProcessor"
 */
object TLEStreamProcessor {

  // ── Configuration ────────────────────────────────────────────────────────────
  val KAFKA_BOOTSTRAP        = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS",  "localhost:19092")
  val KAFKA_TOPIC            = sys.env.getOrElse("KAFKA_TOPIC",              "tle-raw")
  val HDFS_NAMENODE          = sys.env.getOrElse("HDFS_NAMENODE",            "localhost")
  val WEBHDFS_PORT           = sys.env.getOrElse("WEBHDFS_PORT",             "9870")
  val CHECKPOINT_PATH        = sys.env.getOrElse("CHECKPOINT_PATH",          "/tmp/spark-checkpoints/tle-stream")
  val MIN_ALTITUDE_KM        = sys.env.getOrElse("MIN_ALTITUDE_KM",          "150.0").toDouble
  val MAX_TLE_AGE_DAYS       = sys.env.getOrElse("MAX_TLE_AGE_DAYS",         "30").toInt
  val TRIGGER_INTERVAL       = sys.env.getOrElse("TRIGGER_INTERVAL",         "10 seconds")

  // HDFS paths (WebHDFS scheme so Spark can write directly)
  val HDFS_VECTORS_PATH      = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT/space-debris-webhdfs/state-vectors-stream"
  val HDFS_STOPPED_PATH      = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT/space-debris-webhdfs/stopped-tracking"

  // ── Kafka message schema ──────────────────────────────────────────────────────
  val kafkaSchema: StructType = new StructType()
    .add("norad_id",    StringType)
    .add("epoch",       StringType)
    .add("tle_line1",   StringType)
    .add("tle_line2",   StringType)
    .add("ingested_at", StringType)

  // ── SGP4 UDF via Orekit ───────────────────────────────────────────────────────
  // Returns Array[Double](pos_x, pos_y, pos_z, vel_x, vel_y, vel_z)
  // in km / km·s⁻¹ (ECI / GCRF frame).  NaN on any error.
  val sgp4UDF = udf((line1: String, line2: String) => {
    try {
      if (line1 == null || line2 == null || line1.trim.isEmpty || line2.trim.isEmpty)
        Array.fill(6)(Double.NaN)
      else {
        val tle        = new TLE(line1.trim, line2.trim)
        val propagator = TLEPropagator.selectExtrapolator(tle)
        val pv         = propagator.getPVCoordinates(tle.getDate, FramesFactory.getGCRF)
        val pos        = pv.getPosition
        val vel        = pv.getVelocity
        Array(
          pos.getX / 1000.0, pos.getY / 1000.0, pos.getZ / 1000.0,
          vel.getX / 1000.0, vel.getY / 1000.0, vel.getZ / 1000.0
        )
      }
    } catch { case _: Exception => Array.fill(6)(Double.NaN) }
  })

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("🚀 TLE STREAM PROCESSOR  (Scala + Spark + Orekit SGP4)")
    println("=" * 70)
    println(s"📥 Kafka         : $KAFKA_BOOTSTRAP  →  $KAFKA_TOPIC")
    println(s"📤 Vectors HDFS  : $HDFS_VECTORS_PATH")
    println(s"🛑 Stopped HDFS  : $HDFS_STOPPED_PATH")
    println(s"📍 Checkpoint    : $CHECKPOINT_PATH")
    println(s"⬇️  Min altitude  : $MIN_ALTITUDE_KM km")
    println(s"📅 Max TLE age   : $MAX_TLE_AGE_DAYS days")
    println("=" * 70 + "\n")

    // ── 1. Orekit initialisation ────────────────────────────────────────────────
    val orekitDir = new File("orekit-data")
    if (!orekitDir.exists()) {
      println("❌ orekit-data/ not found — aborting")
      System.exit(1)
    }
    DataContext.getDefault().getDataProvidersManager()
      .addProvider(new DirectoryCrawler(orekitDir))
    println("✅ Orekit initialised")

    // ── 2. Spark session ────────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("TLE-Stream-Processor-Scala")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.hadoop.hadoop.job.ugi",   "root")
      .config("spark.hadoop.user.name",        "root")
      .config("spark.hadoop.dfs.replication",  "1")
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "root")
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("✅ Spark session created\n")

    // ── 3. Kafka source ─────────────────────────────────────────────────────────
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets",    "latest")
      .option("failOnDataLoss",     "false")
      .option("maxOffsetsPerTrigger", "10000")
      .load()

    // ── 4. Parse JSON + compute SGP4 vectors ────────────────────────────────────
    val parsed = rawStream
      .select(
        from_json(col("value").cast("string"), kafkaSchema).alias("d"),
        col("timestamp").alias("kafka_ts")
      )
      .select("d.*", "kafka_ts")

    val withVectors = parsed
      .withColumn("sv", sgp4UDF($"tle_line1", $"tle_line2"))
      .withColumn("pos_x",  $"sv"(0)).withColumn("pos_y",  $"sv"(1)).withColumn("pos_z",  $"sv"(2))
      .withColumn("vel_x",  $"sv"(3)).withColumn("vel_y",  $"sv"(4)).withColumn("vel_z",  $"sv"(5))
      .drop("sv")
      .withColumn("altitude_km",
        sqrt($"pos_x" * $"pos_x" + $"pos_y" * $"pos_y" + $"pos_z" * $"pos_z") - lit(6371.0))
      .withColumn("velocity_kms",
        sqrt($"vel_x" * $"vel_x" + $"vel_y" * $"vel_y" + $"vel_z" * $"vel_z"))
      .withColumn("processed_at", current_timestamp())
      // TLE age in days
      .withColumn("tle_age_days",
        datediff(current_timestamp(), to_timestamp($"epoch")))

    // ── 5. Tracking stop conditions (mirrors reference project) ─────────────────
    //   STOPPED_SGP4_ERROR    : SGP4 returned NaN
    //   STOPPED_LOW_ALTITUDE  : altitude < MIN_ALTITUDE_KM (de-orbit threshold)
    //   STOPPED_STALE_TLE     : TLE older than MAX_TLE_AGE_DAYS
    //   ACTIVE                : everything else
    val classified = withVectors
      .withColumn("tracking_status",
        when(isnan($"pos_x") || $"pos_x".isNull,               lit("STOPPED_SGP4_ERROR"))
        .when($"altitude_km" < lit(MIN_ALTITUDE_KM),            lit("STOPPED_LOW_ALTITUDE"))
        .when($"tle_age_days" > lit(MAX_TLE_AGE_DAYS),          lit("STOPPED_STALE_TLE"))
        .otherwise(                                             lit("ACTIVE"))
      )

    val active  = classified.filter($"tracking_status" === "ACTIVE")
    val stopped = classified.filter($"tracking_status" =!= "ACTIVE")

    // ── 6a. Write active vectors to HDFS as Parquet, partitioned by date ─────────
    //        (same pattern as reference spark_sgp4_to_hdfs.py)
    val activeQuery = active
      .withColumn("partition_date", to_date($"processed_at"))
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", HDFS_VECTORS_PATH)
      .option("checkpointLocation", s"$CHECKPOINT_PATH/active-vectors")
      .partitionBy("partition_date")
      .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
      .start()

    println(s"✅ Active-vectors sink  → $HDFS_VECTORS_PATH  (Parquet, partitioned by date)")

    // ── 6b. Write stopped-tracking log to HDFS (audit / analysis) ────────────────
    val stoppedQuery = stopped
      .select("norad_id", "epoch", "altitude_km", "tle_age_days", "tracking_status", "processed_at")
      .withColumn("partition_status", $"tracking_status")
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", HDFS_STOPPED_PATH)
      .option("checkpointLocation", s"$CHECKPOINT_PATH/stopped-tracking")
      .partitionBy("partition_status")
      .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
      .start()

    println(s"✅ Stopped-tracking sink → $HDFS_STOPPED_PATH  (Parquet, partitioned by status)")

    // ── 6c. Console sink for live monitoring ────────────────────────────────────
    val consoleQuery = active
      .select("norad_id", "epoch", "altitude_km", "velocity_kms",
              "pos_x", "pos_y", "pos_z", "tracking_status")
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate",  "false")
      .option("numRows",   "5")
      .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
      .start()

    println("\n⏳ Streaming — press Ctrl+C to stop\n" + "-" * 70)

    // ── 7. Await termination ────────────────────────────────────────────────────
    try {
      spark.streams.awaitAnyTermination()
    } catch {
      case _: InterruptedException =>
        println("\n🛑 Interrupt received — stopping streams...")
        spark.streams.active.foreach(_.stop())
        spark.stop()
        println("✅ Stopped cleanly")
    }
  }
}