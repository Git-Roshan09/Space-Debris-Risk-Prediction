import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import java.io.File
import java.net.{HttpURLConnection, URL}
import java.io.{BufferedReader, InputStreamReader}
import org.orekit.data.DataContext
import org.orekit.data.DirectoryCrawler

/**
 * Streaming Collision Detector - Scala Spark
 *
 * Mirrors the reference project's spark_collision_prediction.py in Scala.
 *
 * Runs continuously: every POLL_INTERVAL_SECS it reads the latest state-vector
 * Parquet files written by TLEStreamProcessor, performs pairwise collision
 * detection (SAT-SAT + SAT-DEB, DEB-DEB excluded), and:
 *   1. Writes timestamped collision-alerts batch to HDFS (CSV)
 *   2. Publishes ALL alerts to Kafka topic `collision-alerts`
 *
 * Risk thresholds (same as reference project):
 *   CRITICAL  ≤ 1 km
 *   HIGH      ≤ 20 km
 *   MEDIUM    ≤ 35 km
 *   LOW       ≤ 50 km  (= COLLISION_THRESHOLD_KM)
 *
 * Run:
 *   sbt "runMain StreamingCollisionDetector"
 */
object StreamingCollisionDetector {

  // ── Configuration ───────────────────────────────────────────────────────────
  val HDFS_NAMENODE          = sys.env.getOrElse("HDFS_NAMENODE",               "localhost")
  val WEBHDFS_PORT           = sys.env.getOrElse("WEBHDFS_PORT",                "9870")
  val HDFS_USER              = sys.env.getOrElse("HDFS_USER",                   "root")

  // State vectors written by TLEStreamProcessor (Parquet, partitioned by date)
  val HDFS_VECTORS_PATH      = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT/space-debris-webhdfs/state-vectors-stream"

  // Collision alerts output
  val HDFS_COLLISIONS_PATH   = "/space-debris-webhdfs/collision-alerts"

  // Local catalog for SAT / DEBRIS classification
  val LOCAL_CATALOG_PATH     = "Output/space_debris_catalog.csv"

  // Kafka
  val KAFKA_BOOTSTRAP        = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS",     "localhost:19092")
  val KAFKA_ALERTS_TOPIC     = sys.env.getOrElse("KAFKA_ALERTS_TOPIC",          "collision-alerts")

  // Thresholds (km) — match reference project
  val COLLISION_THRESHOLD_KM = sys.env.getOrElse("COLLISION_THRESHOLD_KM",      "50.0").toDouble
  val HIGH_RISK_KM           = sys.env.getOrElse("HIGH_RISK_THRESHOLD_KM",      "20.0").toDouble
  val MEDIUM_RISK_KM         = sys.env.getOrElse("MEDIUM_RISK_THRESHOLD_KM",    "35.0").toDouble
  val MAX_OBJECTS            = sys.env.getOrElse("MAX_OBJECTS",                 "5000").toInt
  val BUCKET_SIZE            = 50.0  // Altitude grid size to prevent OOM

  // How often to re-run detection (seconds)
  val POLL_INTERVAL_SECS     = sys.env.getOrElse("POLL_INTERVAL_SECS",          "120").toInt

  // ── Schema for state-vector Parquet files ────────────────────────────────────
  // Must match TLEStreamProcessor output columns
  val vectorSchema: StructType = new StructType()
    .add("norad_id",        StringType,  true)
    .add("epoch",           StringType,  true)
    .add("pos_x",           DoubleType,  true)
    .add("pos_y",           DoubleType,  true)
    .add("pos_z",           DoubleType,  true)
    .add("vel_x",           DoubleType,  true)
    .add("vel_y",           DoubleType,  true)
    .add("vel_z",           DoubleType,  true)
    .add("altitude_km",     DoubleType,  true)
    .add("velocity_kms",    DoubleType,  true)
    .add("tracking_status", StringType,  true)
    .add("processed_at",    StringType,  true)
    .add("partition_date",  StringType,  true)

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("🛰️  STREAMING COLLISION DETECTOR  (Scala + Spark)")
    println("=" * 70)
    println(s"📥 Vectors HDFS  : $HDFS_VECTORS_PATH")
    println(s"📤 Alerts HDFS   : $HDFS_COLLISIONS_PATH")
    println(s"📡 Kafka topic   : $KAFKA_ALERTS_TOPIC")
    println(s"📏 Threshold     : $COLLISION_THRESHOLD_KM km")
    println(s"🔴 High risk     : ≤ $HIGH_RISK_KM km")
    println(s"🟡 Medium risk   : ≤ $MEDIUM_RISK_KM km")
    println(s"⏱  Poll interval : every $POLL_INTERVAL_SECS s")
    println("=" * 70 + "\n")

    // ── Orekit init (context required for class loading) ────────────────────────
    val orekitDir = new File("orekit-data")
    if (orekitDir.exists()) {
      DataContext.getDefault().getDataProvidersManager()
        .addProvider(new DirectoryCrawler(orekitDir))
      println("✅ Orekit data context initialised")
    }

    // ── Spark session ────────────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("SpaceDebris-StreamingCollisionDetector")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled",                    "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.hadoop.hadoop.job.ugi",                   "root")
      .config("spark.hadoop.user.name",                        "root")
      .config("spark.hadoop.dfs.replication",                  "1")
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "root")
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("✅ Spark session created\n")

    // ── Load debris catalog for SAT/DEBRIS classification ──────────────────────
    val debrisIds: Set[Int] = loadDebrisCatalog(LOCAL_CATALOG_PATH, spark)
    println(s"📋 Loaded ${debrisIds.size} debris NORAD IDs from catalog\n")
    val debrisBroadcast = spark.sparkContext.broadcast(debrisIds)

    val classifyUDF = udf((noradIdStr: String) => {
      try {
        val id = noradIdStr.toInt
        if (debrisBroadcast.value.contains(id)) "DEBRIS" else "SATELLITE"
      } catch { case _: Exception => "UNKNOWN" }
    })

    // ── Main detection loop ──────────────────────────────────────────────────────
    var iteration = 1
    var running   = true

    while (running) {
      println(s"\n${"─" * 70}")
      println(s"🔄 Detection cycle #$iteration  [${java.time.LocalDateTime.now()}]")
      println(s"${"─" * 70}")

      try {
        // ── 1. Read latest state vectors from HDFS ──────────────────────────────
        val dfRaw: DataFrame = readStateVectors(spark)

        if (dfRaw.isEmpty) {
          println("⚠️  No state-vector data in HDFS — waiting for TLEStreamProcessor...")
        } else {
          // ── 2. Deduplicate: one snapshot per object (latest processed_at) ───────
          val windowByNorad = Window.partitionBy("norad_id").orderBy(desc("processed_at"))
          val dfLatest = dfRaw
            .withColumn("rn", row_number().over(windowByNorad))
            .filter(col("rn") === 1)
            .drop("rn")
            .withColumn("object_type", classifyUDF(col("norad_id")))
            .limit(MAX_OBJECTS)
            .cache()

          val total  = dfLatest.count()
          val satCnt = dfLatest.filter(col("object_type") === "SATELLITE").count()
          val debCnt = dfLatest.filter(col("object_type") === "DEBRIS").count()
          println(s"📊 Objects loaded : $total  (🛰️ satellites=$satCnt  🗑️ debris=$debCnt)")

          if (total >= 2) {
            // ── 3. Pairwise collision detection ─────────────────────────────────
            val dfCollisions: Option[DataFrame] = detectAllPairs(spark, dfLatest)

            dfCollisions match {
              case None =>
                println("✅ No close-approach pairs found this cycle.")
              case Some(df) =>
                val n = df.count()
                println(s"\n📊 Close-approach pairs detected: $n")

                if (n > 0) {
                  // Risk breakdown
                  df.groupBy("risk_level").count().collect()
                    .sortBy(r => riskOrder(r.getString(0)))
                    .foreach(r => println(s"   ${r.getString(0)}: ${r.getLong(1)}"))

                  // ── 4. Write to HDFS ─────────────────────────────────────────
                  writeCollisionsToHDFS(df)

                  // ── 5. Publish to Kafka ──────────────────────────────────────
                  publishAlertsToKafka(spark, df)
                }
            }

            dfLatest.unpersist()
          } else {
            println("⚠️  Not enough objects for cross-join (need ≥ 2) — skipping.")
          }
        }
      } catch {
        case e: InterruptedException =>
          println("🛑 Interrupted — exiting detection loop.")
          running = false
        case e: Exception =>
          println(s"❌ Error in detection cycle #$iteration: ${e.getMessage.take(120)}")
      }

      if (running) {
        println(s"\n⏱  Next cycle in $POLL_INTERVAL_SECS s — Ctrl+C to stop")
        try { Thread.sleep(POLL_INTERVAL_SECS * 1000L) }
        catch { case _: InterruptedException => running = false }
        iteration += 1
      }
    }

    spark.stop()
    println("\n✅ StreamingCollisionDetector stopped cleanly.")
  }

  // ── Read state vectors from HDFS Parquet (TLEStreamProcessor output) ────────
  def readStateVectors(spark: SparkSession): DataFrame = {
    try {
      spark.read
        .option("mergeSchema", "true")
        .parquet(s"$HDFS_VECTORS_PATH/")
        .filter(
          col("pos_x").isNotNull && !isnan(col("pos_x")) &&
          col("pos_y").isNotNull && !isnan(col("pos_y")) &&
          col("pos_z").isNotNull && !isnan(col("pos_z"))
        )
    } catch {
      case e: Exception =>
        println(s"   ⚠️  Could not read state vectors: ${e.getMessage.take(80)}")
        spark.emptyDataFrame
    }
  }

  // ── Pairwise detection: SAT-SAT + SAT-DEB ───────────────────────────────────
  def detectAllPairs(spark: SparkSession, df: DataFrame): Option[DataFrame] = {
    val sats = df.filter(col("object_type").isin("SATELLITE", "UNKNOWN")).cache()
    val debs = df.filter(col("object_type") === "DEBRIS").cache()
    val satCnt = sats.count()
    val debCnt = debs.count()

    var result: Option[DataFrame] = None

    if (satCnt >= 2) {
      println("   🔍 SAT-SAT pairs...")
      val satSat = detectPairs(spark, sats, sats, "SAT-SAT")
      result = Some(satSat)
    }
    if (satCnt > 0 && debCnt > 0) {
      println("   🔍 SAT-DEB pairs...")
      val satDeb = detectPairs(spark, sats, debs, "SAT-DEB")
      result = result.map(_.union(satDeb)).orElse(Some(satDeb))
    }

    sats.unpersist()
    debs.unpersist()
    result
  }

  // ── Core pair detection (mirrors reference _detect_pairs) ───────────────────
  def detectPairs(
    spark: SparkSession,
    df1: DataFrame,
    df2: DataFrame,
    collisionType: String
  ): DataFrame = {

    // --- Optimized Bucketed Join ---
    // Round altitude to nearest bucket (e.g., 50km shells)
    val df1Bucketed = df1.toDF(df1.columns.map(c => s"a_$c"): _*)
      .withColumn("bucket", floor(col("a_altitude_km") / BUCKET_SIZE))
    
    // For the right side, explode into 3 buckets to handle boundary cases
    val df2Expanded = df2.toDF(df2.columns.map(c => s"b_$c"): _*)
      .withColumn("bucket", explode(array(
        floor(col("b_altitude_km") / BUCKET_SIZE),
        floor(col("b_altitude_km") / BUCKET_SIZE) - 1,
        floor(col("b_altitude_km") / BUCKET_SIZE) + 1
      )))

    val joined = df1Bucketed.join(broadcast(df2Expanded), Seq("bucket"))
      .filter(if (collisionType == "SAT-SAT") col("a_norad_id") < col("b_norad_id") else lit(true))
      .withColumn(
        "distance_km",
        sqrt(
          pow(col("b_pos_x") - col("a_pos_x"), 2) +
          pow(col("b_pos_y") - col("a_pos_y"), 2) +
          pow(col("b_pos_z") - col("a_pos_z"), 2)
        )
      ).filter(col("distance_km") <= COLLISION_THRESHOLD_KM)
      .drop("bucket")
      .dropDuplicates("a_norad_id", "b_norad_id")

    val result = joined
      .withColumn("risk_level",
        when(col("distance_km") <= 1.0,             lit("CRITICAL"))
        .when(col("distance_km") <= HIGH_RISK_KM,   lit("HIGH"))
        .when(col("distance_km") <= MEDIUM_RISK_KM, lit("MEDIUM"))
        .otherwise(                                  lit("LOW")))
      .withColumn("collision_probability",
        when(col("distance_km") <= 0.01, lit(1.0))
        .otherwise(lit(1.0) / (lit(1.0) + col("distance_km") * col("distance_km"))))
      .withColumn("relative_velocity_kms",
        col("a_velocity_kms") + col("b_velocity_kms"))
      .withColumn("collision_type",      lit(collisionType))
      .withColumn("detection_timestamp", current_timestamp())
      .select(
        col("a_norad_id").alias("norad_id_1"),
        col("b_norad_id").alias("norad_id_2"),
        col("a_epoch").alias("epoch_1"),
        col("b_epoch").alias("epoch_2"),
        col("a_pos_x").alias("pos_x_1"),
        col("a_pos_y").alias("pos_y_1"),
        col("a_pos_z").alias("pos_z_1"),
        col("b_pos_x").alias("pos_x_2"),
        col("b_pos_y").alias("pos_y_2"),
        col("b_pos_z").alias("pos_z_2"),
        col("a_altitude_km").alias("altitude_km_1"),
        col("b_altitude_km").alias("altitude_km_2"),
        col("a_velocity_kms").alias("speed_kms_1"),
        col("b_velocity_kms").alias("speed_kms_2"),
        col("a_object_type").alias("type_1"),
        col("b_object_type").alias("type_2"),
        col("distance_km"),
        col("relative_velocity_kms"),
        col("risk_level"),
        col("collision_probability"),
        col("collision_type"),
        col("detection_timestamp"),
        // Midpoint (ECI) for 3D globe
        ((col("a_pos_x") + col("b_pos_x")) / 2.0).alias("approach_pos_x"),
        ((col("a_pos_y") + col("b_pos_y")) / 2.0).alias("approach_pos_y"),
        ((col("a_pos_z") + col("b_pos_z")) / 2.0).alias("approach_pos_z")
      )

    val n = result.count()
    println(s"      $collisionType: $n pairs within $COLLISION_THRESHOLD_KM km")
    result
  }

  // ── Write collision alerts to HDFS ──────────────────────────────────────────
  def writeCollisionsToHDFS(df: DataFrame): Unit = {
    import java.time.format.DateTimeFormatter
    import java.time.LocalDateTime
    val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val webHdfsPath = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT$HDFS_COLLISIONS_PATH/stream_$ts"
    try {
      df.orderBy(asc("distance_km"))
        .write.mode("overwrite")
        .option("header", "true")
        .csv(webHdfsPath)
      println(s"   💾 Alerts written to HDFS: $HDFS_COLLISIONS_PATH/stream_$ts")
    } catch {
      case e: Exception =>
        println(s"   ⚠️  HDFS write failed (${e.getMessage.take(60)}) — falling back to local")
        val localPath = s"Output/stream_collision_alerts_$ts"
        new File("Output").mkdirs()
        df.orderBy(asc("distance_km"))
          .write.mode("overwrite")
          .option("header", "true")
          .csv(localPath)
        println(s"   💾 Alerts written locally: $localPath")
    }
  }

  // ── Publish alerts to Kafka ──────────────────────────────────────────────────
  def publishAlertsToKafka(spark: SparkSession, df: DataFrame): Unit = {
    try {
      val count = df.count()
      if (count == 0) return
      df.selectExpr("CAST(norad_id_1 AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_ALERTS_TOPIC)
        .save()
      println(s"   📡 Published $count alerts → Kafka topic: $KAFKA_ALERTS_TOPIC")
    } catch {
      case e: Exception =>
        println(s"   ⚠️  Kafka publish skipped (${e.getMessage.take(60)})")
    }
  }

  // ── Load debris catalog ──────────────────────────────────────────────────────
  def loadDebrisCatalog(path: String, spark: SparkSession): Set[Int] = {
    val f = new File(path)
    if (!f.exists()) {
      println(s"⚠️  Catalog not found at $path — all objects treated as SATELLITE")
      return Set.empty
    }
    val df = spark.read.option("header", "true").csv(path)
    if (!df.columns.contains("NORAD_CAT_ID")) return Set.empty
    df.select("NORAD_CAT_ID").filter(col("NORAD_CAT_ID").isNotNull).collect()
      .flatMap { row => try Some(row.getString(0).toInt) catch { case _: Exception => None } }
      .toSet
  }

  // ── Risk display ordering ────────────────────────────────────────────────────
  private def riskOrder(level: String): Int = level match {
    case "CRITICAL" => 0; case "HIGH" => 1; case "MEDIUM" => 2; case _ => 3
  }
}
