import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import java.io.File
import java.net.{HttpURLConnection, URL}
import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import org.orekit.data.DataContext
import org.orekit.data.DirectoryCrawler

/**
 * Collision Detector - Spark Batch Job
 *
 * Reads state vectors from HDFS (produced by TLEProcessor / TLEStreamProcessor),
 * performs pairwise distance calculations, classifies risk levels, and writes
 * collision alerts back to HDFS as CSV.
 *
 * Flow: HDFS state-vectors → Spark cross-join → collision_alerts CSV → HDFS
 *
 * Risk thresholds (matches reference spark_collision_prediction.py defaults):
 *   CRITICAL  : distance <= 1 km
 *   HIGH      : distance <= 20 km
 *   MEDIUM    : distance <= 35 km
 *   LOW       : distance <= 50 km   (configurable via COLLISION_THRESHOLD_KM)
 *
 * Run:
 *   sbt "runMain CollisionDetector"
 */
object CollisionDetector {

  // ─── Configuration ─────────────────────────────────────────────────────────
  val HDFS_NAMENODE   = sys.env.getOrElse("HDFS_NAMENODE",          "localhost")
  val WEBHDFS_PORT    = sys.env.getOrElse("WEBHDFS_PORT",           "9870")
  val HDFS_USER       = sys.env.getOrElse("HDFS_USER",              "root")
  val HDFS_BASE_PATH  = "/space-debris-webhdfs"

  // Input: state vectors written by TLEProcessor (one CSV per NORAD ID)
  val HDFS_STATE_VECTORS_PATH = s"$HDFS_BASE_PATH/state-vectors"

  // Input: streaming state vectors written by TLEStreamProcessor (batch sub-dirs)
  val HDFS_STREAM_VECTORS_PATH = s"$HDFS_BASE_PATH/state-vectors-stream"

  // Output: collision alerts CSV
  val HDFS_COLLISIONS_PATH = s"$HDFS_BASE_PATH/collision-alerts"

  // Local catalog for debris classification
  val LOCAL_CATALOG_PATH = "Output/space_debris_catalog.csv"

  // Thresholds (km)
  val COLLISION_THRESHOLD_KM = sys.env.getOrElse("COLLISION_THRESHOLD_KM",  "50.0").toDouble
  val HIGH_RISK_KM            = sys.env.getOrElse("HIGH_RISK_THRESHOLD_KM",  "20.0").toDouble
  val MEDIUM_RISK_KM          = sys.env.getOrElse("MEDIUM_RISK_THRESHOLD_KM","35.0").toDouble

  // Which input to use: "batch" (TLEProcessor output) or "stream" (TLEStreamProcessor output)
  val INPUT_MODE = sys.env.getOrElse("INPUT_MODE", "batch")

  // Maximum number of state vectors to load per run (for performance)
  val MAX_OBJECTS = sys.env.getOrElse("MAX_OBJECTS", "5000").toInt

  // Kafka for publishing collision alerts (mirrors reference project)
  val KAFKA_BOOTSTRAP       = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
  val KAFKA_ALERTS_TOPIC    = sys.env.getOrElse("KAFKA_ALERTS_TOPIC",      "collision-alerts")

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("🛰️  COLLISION DETECTOR (Scala + Spark)")
    println("=" * 70)
    println(s"📥 Input mode:       $INPUT_MODE")
    println(s"📏 Threshold:        $COLLISION_THRESHOLD_KM km")
    println(s"🔴 High risk:        <= $HIGH_RISK_KM km")
    println(s"🟡 Medium risk:      <= $MEDIUM_RISK_KM km")
    println(s"🟢 Low risk:         <= $COLLISION_THRESHOLD_KM km")
    println(s"📤 Output:           HDFS $HDFS_COLLISIONS_PATH")
    println(s"🔢 Max objects:      $MAX_OBJECTS")
    println("=" * 70 + "\n")

    // ── Init Orekit (needed only if we were propagating, kept for consistency) ──
    val orekitData = new File("orekit-data")
    if (orekitData.exists()) {
      val manager = DataContext.getDefault().getDataProvidersManager()
      manager.addProvider(new DirectoryCrawler(orekitData))
      println("✅ Orekit data context initialised")
    } else {
      println("⚠️  orekit-data folder not found — skipping Orekit init (not needed for collision geometry)")
    }

    // ── Spark session ──────────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("SpaceDebris-CollisionDetector")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled",                       "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled",    "true")
      .config("spark.hadoop.hadoop.job.ugi",                      "root")
      .config("spark.hadoop.user.name",                           "root")
      .config("spark.hadoop.dfs.replication",                     "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("✅ Spark session created\n")

    System.setProperty("HADOOP_USER_NAME", "root")

    // ── Step 1: Load debris catalog for classification ─────────────────────────
    val debrisNoradIds: Set[Int] = loadDebrisCatalog(LOCAL_CATALOG_PATH, spark)
    println(s"📋 Loaded ${debrisNoradIds.size} debris NORAD IDs from catalog\n")

    // ── Step 2: Read state vectors from HDFS ──────────────────────────────────
    val dfStateVectors: DataFrame = loadStateVectors(spark, debrisNoradIds)

    val totalObjects = dfStateVectors.count()
    println(s"📊 Total objects loaded: $totalObjects\n")

    if (totalObjects < 2) {
      println("⚠️  Not enough objects for collision detection (need >= 2). Exiting.")
      spark.stop()
      return
    }

    // ── Step 3: Pairwise collision detection ──────────────────────────────────
    println("🔍 Running pairwise collision detection...")
    println(s"   Cross-joining SAT-SAT and SAT-DEB pairs (DEB-DEB excluded)...")

    val dfSatellites = dfStateVectors.filter(col("object_type") === "SATELLITE").cache()
    val dfDebris     = dfStateVectors.filter(col("object_type") === "DEBRIS").cache()

    val satCount = dfSatellites.count()
    val debCount = dfDebris.count()
    println(s"   🛰️  Satellites: $satCount   🗑️  Debris: $debCount\n")

    var allCollisions: Option[DataFrame] = None

    // SAT-SAT pairs
    if (satCount >= 2) {
      println("   → Detecting SAT-SAT pairs...")
      val satSat = detectPairs(spark, dfSatellites, dfSatellites, "SAT-SAT")
      allCollisions = Some(satSat)
    }

    // SAT-DEB pairs
    if (satCount > 0 && debCount > 0) {
      println("   → Detecting SAT-DEB pairs...")
      val satDeb = detectPairs(spark, dfSatellites, dfDebris, "SAT-DEB")
      allCollisions = allCollisions match {
        case Some(existing) => Some(existing.union(satDeb))
        case None           => Some(satDeb)
      }
    }

    dfSatellites.unpersist()
    dfDebris.unpersist()

    allCollisions match {
      case None =>
        println("✅ No collision pairs to analyse (insufficient data).")
      case Some(dfCollisions) =>
        val collisionCount = dfCollisions.count()
        println(s"\n📊 Total close-approach pairs detected: $collisionCount")

        if (collisionCount > 0) {
          // Risk breakdown
          val riskBreakdown = dfCollisions.groupBy("risk_level").count().collect()
          riskBreakdown.foreach(r => println(s"   ${r.getString(0)}: ${r.getLong(1)} pairs"))

          // ── Step 4: Write to HDFS ────────────────────────────────────────────
          println("\n💾 Writing collision alerts to HDFS...")
          writeCollisionsToHDFS(dfCollisions)

          // ── Step 5: Publish CRITICAL + HIGH alerts to Kafka ──────────────────
          println("\n📡 Publishing high-risk alerts to Kafka...")
          publishAlertsToKafka(spark, dfCollisions)
        } else {
          println("✅ No objects found within collision threshold — space is safe!")
        }
    }

    println("\n" + "=" * 70)
    println("🎯 COLLISION DETECTION COMPLETE")
    println("=" * 70)

    spark.stop()
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Load state vectors from HDFS
  // ───────────────────────────────────────────────────────────────────────────
  def loadStateVectors(spark: SparkSession, debrisIds: Set[Int]): DataFrame = {
    import spark.implicits._

    // Schema produced by TLEProcessor / TLEStreamProcessor
    val batchSchema = new StructType()
      .add("NORAD_ID",     StringType,  true)
      .add("EPOCH",        StringType,  true)
      .add("POS_X",        DoubleType,  true)
      .add("POS_Y",        DoubleType,  true)
      .add("POS_Z",        DoubleType,  true)
      .add("VEL_X",        DoubleType,  true)
      .add("VEL_Y",        DoubleType,  true)
      .add("VEL_Z",        DoubleType,  true)

    val streamSchema = new StructType()
      .add("norad_id",     StringType,  true)
      .add("epoch",        StringType,  true)
      .add("pos_x",        DoubleType,  true)
      .add("pos_y",        DoubleType,  true)
      .add("pos_z",        DoubleType,  true)
      .add("vel_x",        DoubleType,  true)
      .add("vel_y",        DoubleType,  true)
      .add("vel_z",        DoubleType,  true)
      .add("processed_at", StringType,  true)

    val rawDF: DataFrame = if (INPUT_MODE == "stream") {
      println(s"📡 Reading streaming state vectors from HDFS: $HDFS_STREAM_VECTORS_PATH")
      val webHdfsPath = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT$HDFS_STREAM_VECTORS_PATH"
      try {
        spark.read
          .option("header", "true")
          .option("mergeSchema", "true")
          .schema(streamSchema)
          .csv(s"$webHdfsPath/batch_*/")
          .withColumnRenamed("norad_id",  "NORAD_ID")
          .withColumnRenamed("epoch",     "EPOCH")
          .withColumnRenamed("pos_x",     "POS_X")
          .withColumnRenamed("pos_y",     "POS_Y")
          .withColumnRenamed("pos_z",     "POS_Z")
          .withColumnRenamed("vel_x",     "VEL_X")
          .withColumnRenamed("vel_y",     "VEL_Y")
          .withColumnRenamed("vel_z",     "VEL_Z")
      } catch {
        case e: Exception =>
          println(s"⚠️  Could not read stream vectors: ${e.getMessage}")
          spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], batchSchema)
      }
    } else {
      println(s"📡 Reading batch state vectors from HDFS: $HDFS_STATE_VECTORS_PATH")
      // Read all per-NORAD-ID CSV files
      val allFiles = listWebHDFSDir(HDFS_STATE_VECTORS_PATH)
        .filter(_.endsWith("_state_vectors.csv"))
        .take(MAX_OBJECTS)

      println(s"   Found ${allFiles.length} state vector files")

      if (allFiles.isEmpty) {
        println("⚠️  No state vector files found — check HDFS path")
        return spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], batchSchema)
      }

      val webHdfsBase = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT"
      val paths = allFiles.map(f => s"$webHdfsBase$f")

      spark.read
        .option("header", "true")
        .schema(batchSchema)
        .csv(paths: _*)
    }

    // Normalise, deduplicate, classify, compute altitude & speed
    val debrisBroadcast = spark.sparkContext.broadcast(debrisIds)

    val classifyUDF = udf((noradIdStr: String) => {
      try {
        val id = noradIdStr.toInt
        if (debrisBroadcast.value.contains(id)) "DEBRIS" else "SATELLITE"
      } catch { case _: Exception => "UNKNOWN" }
    })

    rawDF
      .filter(
        col("NORAD_ID").isNotNull &&
        col("POS_X").isNotNull && !isnan(col("POS_X")) &&
        col("POS_Y").isNotNull && !isnan(col("POS_Y")) &&
        col("POS_Z").isNotNull && !isnan(col("POS_Z"))
      )
      .dropDuplicates(Seq("NORAD_ID"))       // one snapshot per object
      .withColumn("object_type", classifyUDF(col("NORAD_ID")))
      .withColumn(
        "altitude_km",
        sqrt(col("POS_X") * col("POS_X") + col("POS_Y") * col("POS_Y") + col("POS_Z") * col("POS_Z")) - lit(6371.0)
      )
      .withColumn(
        "speed_kms",
        sqrt(col("VEL_X") * col("VEL_X") + col("VEL_Y") * col("VEL_Y") + col("VEL_Z") * col("VEL_Z"))
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Pairwise close-approach detection
  // ───────────────────────────────────────────────────────────────────────────
  def detectPairs(
    spark: SparkSession,
    df1: DataFrame,
    df2: DataFrame,
    collisionType: String
  ): DataFrame = {
    import spark.implicits._

    val left  = df1.toDF(df1.columns.map(c => s"a_$c"): _*)
    val right = df2.toDF(df2.columns.map(c => s"b_$c"): _*)

    // Cross-join and filter self-pairs (for SAT-SAT, keep only a < b)
    val joined = if (collisionType == "SAT-SAT") {
      left.crossJoin(right).filter(col("a_NORAD_ID") < col("b_NORAD_ID"))
    } else {
      left.crossJoin(right)
    }

    // Euclidean distance
    val withDist = joined.withColumn(
      "distance_km",
      sqrt(
        pow(col("b_POS_X") - col("a_POS_X"), 2) +
        pow(col("b_POS_Y") - col("a_POS_Y"), 2) +
        pow(col("b_POS_Z") - col("a_POS_Z"), 2)
      )
    )

    // Filter by threshold
    val withinThreshold = withDist.filter(col("distance_km") <= COLLISION_THRESHOLD_KM)

    // Risk classification + probability
    val classified = withinThreshold
      .withColumn(
        "risk_level",
        when(col("distance_km") <= 1.0,              lit("CRITICAL"))
        .when(col("distance_km") <= HIGH_RISK_KM,    lit("HIGH"))
        .when(col("distance_km") <= MEDIUM_RISK_KM,  lit("MEDIUM"))
        .otherwise(                                   lit("LOW"))
      )
      .withColumn(
        "collision_probability",
        when(col("distance_km") <= 0.01, lit(1.0))
        .otherwise(lit(1.0) / (lit(1.0) + col("distance_km") * col("distance_km")))
      )
      .withColumn(
        "relative_velocity_kms",
        col("a_speed_kms") + col("b_speed_kms")   // scalar approximation
      )
      .withColumn("collision_type",      lit(collisionType))
      .withColumn("detection_timestamp", current_timestamp())

    // Select final output columns
    val result = classified.select(
      col("a_NORAD_ID").alias("norad_id_1"),
      col("b_NORAD_ID").alias("norad_id_2"),
      col("a_EPOCH").alias("epoch_1"),
      col("b_EPOCH").alias("epoch_2"),
      col("a_POS_X").alias("pos_x_1"),
      col("a_POS_Y").alias("pos_y_1"),
      col("a_POS_Z").alias("pos_z_1"),
      col("b_POS_X").alias("pos_x_2"),
      col("b_POS_Y").alias("pos_y_2"),
      col("b_POS_Z").alias("pos_z_2"),
      col("a_altitude_km").alias("altitude_km_1"),
      col("b_altitude_km").alias("altitude_km_2"),
      col("a_speed_kms").alias("speed_kms_1"),
      col("b_speed_kms").alias("speed_kms_2"),
      col("a_object_type").alias("type_1"),
      col("b_object_type").alias("type_2"),
      col("distance_km"),
      col("relative_velocity_kms"),
      col("risk_level"),
      col("collision_probability"),
      col("collision_type"),
      col("detection_timestamp"),
      // Mid-point of the two objects in ECI coords — used by the 3D globe
      ((col("a_POS_X") + col("b_POS_X")) / 2.0).alias("approach_pos_x"),
      ((col("a_POS_Y") + col("b_POS_Y")) / 2.0).alias("approach_pos_y"),
      ((col("a_POS_Z") + col("b_POS_Z")) / 2.0).alias("approach_pos_z")
    )

    val pairCount = result.count()
    println(s"   $collisionType: $pairCount close-approach pairs within $COLLISION_THRESHOLD_KM km")
    result
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Publish CRITICAL + HIGH collision alerts to Kafka
  // Mirrors reference project's publish_to_kafka() in spark_collision_prediction.py
  // ───────────────────────────────────────────────────────────────────────────
  def publishAlertsToKafka(spark: SparkSession, dfCollisions: DataFrame): Unit = {
    import spark.implicits._
    try {
      val alerts = dfCollisions.filter(col("risk_level").isin("CRITICAL", "HIGH", "MEDIUM", "LOW"))
      val count = alerts.count()
      if (count == 0) {
        println("   No alerts to publish")
        return
      }

      // Convert each row to a JSON Kafka message (key = norad_id_1)
      alerts
        .selectExpr(
          "CAST(norad_id_1 AS STRING) AS key",
          "to_json(struct(*))          AS value"
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_ALERTS_TOPIC)
        .save()

      println(s"   ✅ Published $count alerts to Kafka topic: $KAFKA_ALERTS_TOPIC")
    } catch {
      case e: Exception =>
        println(s"   ⚠️  Kafka publish skipped (broker unavailable?): ${e.getMessage.take(80)}")
    }
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Write collision alerts to HDFS via WebHDFS
  // ───────────────────────────────────────────────────────────────────────────
  def writeCollisionsToHDFS(dfCollisions: DataFrame): Unit = {
    import java.time.format.DateTimeFormatter
    import java.time.LocalDateTime

    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val outputPath = s"$HDFS_COLLISIONS_PATH/batch_$timestamp"

    try {
      val webHdfsPath = s"webhdfs://$HDFS_NAMENODE:$WEBHDFS_PORT$outputPath"

      dfCollisions
        .orderBy(asc("distance_km"))
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(webHdfsPath)

      println(s"✅ Collision alerts written to HDFS: $outputPath")
      println(s"   (${dfCollisions.count()} rows)")
    } catch {
      case e: Exception =>
        println(s"⚠️  WebHDFS write failed: ${e.getMessage}")
        println("   Falling back to local output...")

        val localPath = s"Output/collision_alerts_$timestamp.csv"
        new File("Output").mkdirs()

        dfCollisions
          .orderBy(asc("distance_km"))
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(localPath)

        println(s"✅ Collision alerts written locally: $localPath")
    }
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Load debris NORAD IDs from local catalog CSV
  // ───────────────────────────────────────────────────────────────────────────
  def loadDebrisCatalog(path: String, spark: SparkSession): Set[Int] = {
    val f = new File(path)
    if (!f.exists()) {
      println(s"⚠️  Catalog not found at $path — all objects will be classified as SATELLITE")
      return Set.empty
    }

    val df = spark.read
      .option("header", "true")
      .csv(path)

    if (!df.columns.contains("NORAD_CAT_ID")) {
      println(s"⚠️  Catalog has no NORAD_CAT_ID column — cannot classify")
      return Set.empty
    }

    df.select("NORAD_CAT_ID")
      .filter(col("NORAD_CAT_ID").isNotNull)
      .collect()
      .flatMap { row =>
        try Some(row.getString(0).toInt)
        catch { case _: Exception => None }
      }
      .toSet
  }

  // ───────────────────────────────────────────────────────────────────────────
  // WebHDFS helpers
  // ───────────────────────────────────────────────────────────────────────────
  def listWebHDFSDir(hdfsPath: String): Array[String] = {
    try {
      val url = new URL(
        s"http://$HDFS_NAMENODE:$WEBHDFS_PORT/webhdfs/v1$hdfsPath?op=LISTSTATUS&user.name=$HDFS_USER"
      )
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")

      if (conn.getResponseCode != 200) return Array.empty

      val reader = new BufferedReader(new InputStreamReader(conn.getInputStream))
      val response = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString
      reader.close()

      // Simple regex extraction of pathSuffix values
      val pattern = """"pathSuffix"\s*:\s*"([^"]+)"""".r
      pattern.findAllMatchIn(response)
        .map(m => s"$hdfsPath/${m.group(1)}")
        .toArray
    } catch {
      case e: Exception =>
        println(s"⚠️  Could not list HDFS dir $hdfsPath: ${e.getMessage}")
        Array.empty
    }
  }
}
