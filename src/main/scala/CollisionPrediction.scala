import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Collision Prediction Engine (aligned with reference project)
 *
 * Pipeline Steps (matching reference spark_collision_prediction.py):
 *   1. Read SGP4 state vectors from HDFS
 *   2. Classify objects (SATELLITE / DEBRIS / UNKNOWN) using catalog
 *   3. Get latest position per object (windowed dedup)
 *   4. Apply tracking stop conditions (altitude, validity)
 *   5. Detect collision pairs: SAT-SAT, SAT-DEB (DEB-DEB excluded)
 *   6. Classify risk: CRITICAL ≤1km, HIGH ≤20km, MEDIUM ≤35km, LOW ≤50km
 *   7. Compute collision probability (inverse-distance model)
 *   8. Output: HDFS (timestamped batch), Kafka (streaming alerts)
 */
object CollisionPrediction {

  val COLLISION_THRESHOLD_KM   = 50.0
  val CRITICAL_THRESHOLD_KM    = 1.0
  val HIGH_RISK_THRESHOLD_KM   = 20.0   
  val MEDIUM_RISK_THRESHOLD_KM = 35.0   
  val EARTH_RADIUS_KM          = 6371.0
  val MIN_ALTITUDE_KM          = 150.0
  val MAX_ALTITUDE_KM          = 100000.0
  val BUCKET_SIZE              = 50.0   

  val RECENT_HOURS             = sys.env.getOrElse("RECENT_HOURS", "24").toInt

  // Max objects per class to protect against huge cross-joins on low-disk machines
  val MAX_SATELLITES           = sys.env.getOrElse("MAX_SATELLITES", "10000").toInt
  val MAX_DEBRIS               = sys.env.getOrElse("MAX_DEBRIS",     "15000").toInt

  // ============================================================
  // HDFS + KAFKA PATHS
  // ============================================================
  val HDFS_BASE              = "hdfs://localhost:9000/space-debris"
  val HDFS_STATE_VECTORS     = s"$HDFS_BASE/state-vectors"
  val HDFS_CATALOG           = s"$HDFS_BASE/catalog"
  val HDFS_COLLISION_OUTPUT  = s"$HDFS_BASE/collision-predictions"
  val HDFS_STOPPED_TRACKING  = s"$HDFS_BASE/stopped-tracking"
  val HDFS_KMEANS_MODEL      = s"$HDFS_BASE/models/orbit-clustering"
  val HDFS_LR_ALTITUDE_MODEL = s"$HDFS_BASE/models/trajectory-altitude"
  val HDFS_LR_SPEED_MODEL    = s"$HDFS_BASE/models/trajectory-speed"
  val KAFKA_BOOTSTRAP        = "localhost:19092"
  val KAFKA_COLLISION_TOPIC  = "space_debris_collisions"

  def main(args: Array[String]): Unit = {

    val pipelineStart = System.currentTimeMillis()
    val batchTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    println("=" * 70)
    println("  SPACE DEBRIS COLLISION PREDICTION ENGINE")
    println(s"  Batch: $batchTimestamp")
    println("=" * 70)

    // ============================================================
    // 1. INITIALIZE SPARK
    // ============================================================
    val spark = SparkSession.builder()
      .appName("SpaceDebris-CollisionPrediction")
      .master("local[14]")
      .config("spark.driver.memory", "6g")
      .config("spark.driver.maxResultSize", "1g")
      // Reduce shuffle partitions — default 200 is too many for local mode
      .config("spark.sql.shuffle.partitions", "32")
      // Point Spark temp/shuffle dirs to /tmp (usually a separate mount)
      .config("spark.local.dir", "/tmp/spark-local")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      .config("spark.hadoop.dfs.replication", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    try {
      // ============================================================
      // 2. LOAD STATE VECTORS FROM HDFS
      // ============================================================
      println("\n[1/7] Loading state vectors from HDFS...")

      // ── Load all state vectors from live_sv_* files ───────────────────────
      // Row dedup is handled later via row_number().over(partitionBy(NORAD_ID)
      // .orderBy(EPOCH desc)) so we always get the latest position per object.
      // Read ONLY live_sv_* files — legacy part-* files have NORAD_ID as INT32
      // while live_sv_* files write it as STRING. Reading both in one .parquet()
      // call causes SchemaColumnConvertNotSupportedException.
      val rawVectors = spark.read
        .option("mergeSchema", "false")
        .parquet(s"$HDFS_STATE_VECTORS/live_sv_*.parquet")
        .na.drop(Seq("POS_X", "POS_Y", "POS_Z"))
        // Normalise NORAD_ID: cast away any residual INT32 to STRING first
        .withColumn("NORAD_ID", col("NORAD_ID").cast(StringType))
        .select("NORAD_ID", "OBJECT_TYPE", "EPOCH", "POS_X", "POS_Y", "POS_Z",
                "VEL_X", "VEL_Y", "VEL_Z", "ALTITUDE_KM", "SPEED_KMS")

      println(s"  Available columns: ${rawVectors.columns.mkString(", ")}")

      // Compute altitude & velocity if not present
      val enrichedDF = rawVectors
        .withColumn("NORAD_ID", col("NORAD_ID").cast(IntegerType))
        .withColumn("ALTITUDE_KM",
          coalesce(col("ALTITUDE_KM"),
            sqrt(col("POS_X") * col("POS_X") + col("POS_Y") * col("POS_Y") + col("POS_Z") * col("POS_Z")) - lit(EARTH_RADIUS_KM)))
        .withColumn("VELOCITY_KMS",
          coalesce(col("SPEED_KMS"),
            sqrt(col("VEL_X") * col("VEL_X") + col("VEL_Y") * col("VEL_Y") + col("VEL_Z") * col("VEL_Z"))))
        .withColumn("PROCESSING_TIME", current_timestamp())

      val totalRecords = enrichedDF.count()
      println(s"  State vector records loaded: $totalRecords")

      // ============================================================
      // 3. LOAD CATALOG & CLASSIFY OBJECTS (like reference project)
      // ============================================================
      println("\n[2/7] Loading catalog and classifying objects...")

      val catalogDF = spark.read
        .parquet(HDFS_CATALOG)
        .select(
          col("NORAD_CAT_ID").cast(IntegerType).alias("CAT_NORAD_ID"),
          col("OBJECT_NAME"),
          col("COUNTRY"),
          col("RCS_SIZE")
         
        )
        .na.drop(Seq("CAT_NORAD_ID"))

      println(s"  Catalog objects loaded: ${catalogDF.count()}")

      val latestPositions = enrichedDF
        .withColumn("row_num", row_number().over(Window.partitionBy("NORAD_ID").orderBy(col("EPOCH").desc)))
        .filter(col("row_num") === 1)
        .drop("row_num")
        .join(broadcast(catalogDF), col("NORAD_ID") === col("CAT_NORAD_ID"), "left")
        .drop("CAT_NORAD_ID")
          when(col("OBJECT_TYPE") === "DEBRIS", "DEBRIS")
          .when(col("OBJECT_TYPE") === "SATELLITE", "SATELLITE")
          .otherwise("SATELLITE")  // default to SATELLITE rather than discarding
        )

      // Cache AFTER filtering so we only cache the small active set
      latestPositions.cache()
      latestPositions.count() // materialise the cache once here

      // Classification counts from the cached (already materialised) set
      println("  Classification distribution:")
      latestPositions.groupBy("CLASSIFICATION").count().show()

      // ============================================================
      // 5. APPLY TRACKING STOP CONDITIONS
      // ============================================================
      println("[4/7] Applying tracking stop conditions...")

      val activeDF = latestPositions
        .withColumn("TRACKING_STATUS",
          when(col("ALTITUDE_KM") < MIN_ALTITUDE_KM, "STOPPED_LOW_ALTITUDE")
          .when(col("ALTITUDE_KM") > MAX_ALTITUDE_KM, "STOPPED_UNREALISTIC")
          .when(col("ALTITUDE_KM").isNull || col("ALTITUDE_KM").isNaN, "STOPPED_INVALID_DATA")
          .otherwise("ACTIVE")
        )

      val activeObjects = activeDF.filter(col("TRACKING_STATUS") === "ACTIVE").cache()
      val stoppedObjects = activeDF.filter(col("TRACKING_STATUS") =!= "ACTIVE")

      val activeCount = activeObjects.count()
      val stoppedCount = stoppedObjects.count()

      println(s"  Active objects:  $activeCount")
      println(s"  Stopped objects: $stoppedCount")

      // Save stopped tracking log (like reference project)
      if (stoppedCount > 0) {
        stoppedObjects
          .select("NORAD_ID", "EPOCH", "ALTITUDE_KM", "TRACKING_STATUS", "PROCESSING_TIME")
          .coalesce(1)
          .write.mode("overwrite")
          .parquet(s"$HDFS_STOPPED_TRACKING/batch_$batchTimestamp")
        println("  Stopped tracking log saved to HDFS")
      }

      // ============================================================
      // 4b. ORBIT SHELL TAGGING (K-Means model from MLlibTraining)
      // ============================================================
      println("\n[4b/7] Tagging objects with orbit shell (K-Means)...")

      // The K-Means model was trained on [PERIOD, INCLINATION, APOGEE, PERIGEE].
      // We approximate those from the ECI state vectors already in the parquet:
      //   PERIOD  ≈ 2π √(r³/GM) / 60   (minutes)
      //   APOGEE  ≈ PERIGEE ≈ ALTITUDE_KM  (near-circular approximation)
      //   INCLINATION — not stored in state vector, set to 0 as placeholder
      //
      // The cluster index maps to an orbit shell label:
      //   K-Means K=4 → LEO (<2000 km), MEO (2000-20000), GEO (~35786), HEO (elliptical)
      // We re-derive the label from altitude rather than the cluster index because
      // cluster ordering is non-deterministic across training runs.

      val GM_EARTH = 398600.4418   // km³/s²

      val activeWithShell = try {
        val kmeansModel = KMeansModel.load(HDFS_KMEANS_MODEL)

        val orbitAssembler = new VectorAssembler()
          .setInputCols(Array("PERIOD_APPROX", "INCLINATION_APPROX", "ALTITUDE_KM", "ALTITUDE_KM"))
          .setOutputCol("orbit_features")

        val withOrbitalFeatures = activeObjects
          .withColumn("PERIOD_APPROX",
            lit(2.0 * math.Pi) * sqrt(
              pow(col("ALTITUDE_KM") + EARTH_RADIUS_KM, 3) / lit(GM_EARTH)
            ) / lit(60.0)
          )
          .withColumn("INCLINATION_APPROX", lit(0.0))  // placeholder — not in ECI parquet

        val assembled = orbitAssembler.transform(withOrbitalFeatures)
        val predicted = kmeansModel.transform(assembled)

        // Map cluster index → human-readable shell using altitude thresholds
        // (more reliable than raw cluster index which can shift between training runs)
        val taggedDF = predicted
          .withColumn("ORBIT_SHELL",
            when(col("ALTITUDE_KM") < 2000.0,  lit("LEO"))
            .when(col("ALTITUDE_KM") < 20000.0, lit("MEO"))
            .when(col("ALTITUDE_KM") < 40000.0, lit("GEO"))
            .otherwise(lit("HEO"))
          )
          .withColumn("ORBIT_CLUSTER", col("prediction").cast(IntegerType))
          .drop("orbit_features", "PERIOD_APPROX", "INCLINATION_APPROX", "prediction")

        println("  Orbit shell distribution:")
        taggedDF.groupBy("ORBIT_SHELL").count().orderBy("ORBIT_SHELL").show()

        taggedDF

      } catch {
        case e: Exception =>
          println(s"  ⚠️  K-Means model load failed (${e.getMessage}) — skipping shell tagging")
          // Add placeholder columns so downstream code always sees them
          activeObjects
            .withColumn("ORBIT_SHELL",
              when(col("ALTITUDE_KM") < 2000.0,  lit("LEO"))
              .when(col("ALTITUDE_KM") < 20000.0, lit("MEO"))
              .when(col("ALTITUDE_KM") < 40000.0, lit("GEO"))
              .otherwise(lit("HEO"))
            )
            .withColumn("ORBIT_CLUSTER", lit(-1).cast(IntegerType))
      }

      // ============================================================
      // 4c. LINEAR REGRESSION — PREDICTED ALTITUDE & SPEED CROSS-CHECK
      // ============================================================
      println("\n[4c/7] Adding LR-predicted altitude & speed cross-check...")

      val lrAssembler6 = new VectorAssembler()
        .setInputCols(Array("POS_X", "POS_Y", "POS_Z", "VEL_X", "VEL_Y", "VEL_Z"))
        .setOutputCol("lr6_features")

      val lrAssembler3 = new VectorAssembler()
        .setInputCols(Array("POS_X", "POS_Y", "POS_Z"))
        .setOutputCol("lr3_features")

      val activeWithML = try {
        val lrAlt   = LinearRegressionModel.load(HDFS_LR_ALTITUDE_MODEL)
        val lrSpeed = LinearRegressionModel.load(HDFS_LR_SPEED_MODEL)

        val with6   = lrAssembler6.transform(activeWithShell)
        val altPred = lrAlt.transform(with6)
          .withColumn("PREDICTED_ALTITUDE_KM", round(col("prediction"), 3))
          .withColumn("ALTITUDE_DELTA_KM",     round(abs(col("ALTITUDE_KM") - col("PREDICTED_ALTITUDE_KM")), 3))
          .drop("lr6_features", "prediction")

        val with3    = lrAssembler3.transform(altPred)
        val speedPred = lrSpeed.transform(with3)
          .withColumn("PREDICTED_SPEED_KMS", round(col("prediction"), 6))
          .withColumn("SPEED_DELTA_KMS",     round(abs(col("VELOCITY_KMS") - col("PREDICTED_SPEED_KMS")), 6))
          .drop("lr3_features", "prediction")

        println("  LR predictions applied — PREDICTED_ALTITUDE_KM, PREDICTED_SPEED_KMS columns added")
        speedPred

      } catch {
        case e: Exception =>
          println(s"  ⚠️  LR model load failed (${e.getMessage}) — adding null placeholders")
          activeWithShell
            .withColumn("PREDICTED_ALTITUDE_KM", lit(null).cast(DoubleType))
            .withColumn("ALTITUDE_DELTA_KM",     lit(null).cast(DoubleType))
            .withColumn("PREDICTED_SPEED_KMS",   lit(null).cast(DoubleType))
            .withColumn("SPEED_DELTA_KMS",       lit(null).cast(DoubleType))
      }

      // ============================================================
      // 6. COLLISION DETECTION (SAT-SAT, SAT-DEB)
      // ============================================================
      println("\n[5/7] Detecting potential collisions...")
      println(s"  Collision threshold: $COLLISION_THRESHOLD_KM km")
      println(s"  Types: SAT-SAT, SAT-DEB (DEB-DEB excluded per reference)")

      val satellites = activeWithML.filter(col("CLASSIFICATION") === "SATELLITE")
        .limit(MAX_SATELLITES).cache()
      val debris = activeWithML.filter(col("CLASSIFICATION") === "DEBRIS")
        .limit(MAX_DEBRIS).cache()

      val satCount = satellites.count()
      val debCount = debris.count()
      println(s"  Satellites: $satCount (capped at $MAX_SATELLITES)")
      println(s"  Debris:     $debCount (capped at $MAX_DEBRIS)")

      var allCollisions: DataFrame = spark.emptyDataFrame

      // --- SAT-SAT pairs (Optimized with Bucketing to prevent OOM) ---
      if (satCount >= 2) {
        println("  Detecting SAT-SAT pairs...")
        val sat1 = satellites.select(
          col("NORAD_ID").alias("norad_1"), col("OBJECT_NAME").alias("name_1"),
          col("CLASSIFICATION").alias("class_1"),
          col("ORBIT_SHELL").alias("orbit_shell_1"),
          col("POS_X").alias("x1"), col("POS_Y").alias("y1"), col("POS_Z").alias("z1"),
          col("ALTITUDE_KM").alias("alt_1"), col("VELOCITY_KMS").alias("vel_1")
        ).withColumn("bucket", floor(col("alt_1") / BUCKET_SIZE))

        val sat2 = satellites.select(
          col("NORAD_ID").alias("norad_2"), col("OBJECT_NAME").alias("name_2"),
          col("CLASSIFICATION").alias("class_2"),
          col("ORBIT_SHELL").alias("orbit_shell_2"),
          col("POS_X").alias("x2"), col("POS_Y").alias("y2"), col("POS_Z").alias("z2"),
          col("ALTITUDE_KM").alias("alt_2"), col("VELOCITY_KMS").alias("vel_2")
        ).withColumn("bucket", explode(array(
            floor(col("alt_2") / BUCKET_SIZE),
            floor(col("alt_2") / BUCKET_SIZE) - 1,
            floor(col("alt_2") / BUCKET_SIZE) + 1
          )))

        val satSat = sat1.join(sat2, Seq("bucket"))
          .filter(col("norad_1") < col("norad_2"))
          .withColumn("distance_km", sqrt(
            pow(col("x2") - col("x1"), 2) + pow(col("y2") - col("y1"), 2) + pow(col("z2") - col("z1"), 2)
          ))
          .filter(col("distance_km") <= COLLISION_THRESHOLD_KM)
          .withColumn("collision_type", lit("SAT-SAT"))
          .drop("bucket")
          .dropDuplicates("norad_1", "norad_2")

        allCollisions = satSat
        println(s"    SAT-SAT close approaches: ${satSat.count()}")
      }

      // --- SAT-DEB pairs (Optimized with Bucketing) ---
      if (satCount > 0 && debCount > 0) {
        println("  Detecting SAT-DEB pairs...")
        val sat1 = satellites.select(
          col("NORAD_ID").alias("norad_1"), col("OBJECT_NAME").alias("name_1"),
          col("CLASSIFICATION").alias("class_1"),
          col("ORBIT_SHELL").alias("orbit_shell_1"),
          col("POS_X").alias("x1"), col("POS_Y").alias("y1"), col("POS_Z").alias("z1"),
          col("ALTITUDE_KM").alias("alt_1"), col("VELOCITY_KMS").alias("vel_1")
        ).withColumn("bucket", floor(col("alt_1") / BUCKET_SIZE))

        val deb1 = debris.select(
          col("NORAD_ID").alias("norad_2"), col("OBJECT_NAME").alias("name_2"),
          col("CLASSIFICATION").alias("class_2"),
          col("ORBIT_SHELL").alias("orbit_shell_2"),
          col("POS_X").alias("x2"), col("POS_Y").alias("y2"), col("POS_Z").alias("z2"),
          col("ALTITUDE_KM").alias("alt_2"), col("VELOCITY_KMS").alias("vel_2")
        ).withColumn("bucket", explode(array(
            floor(col("alt_2") / BUCKET_SIZE),
            floor(col("alt_2") / BUCKET_SIZE) - 1,
            floor(col("alt_2") / BUCKET_SIZE) + 1
          )))

        val satDeb = sat1.join(deb1, Seq("bucket"))
          .withColumn("distance_km", sqrt(
            pow(col("x2") - col("x1"), 2) + pow(col("y2") - col("y1"), 2) + pow(col("z2") - col("z1"), 2)
          ))
          .filter(col("distance_km") <= COLLISION_THRESHOLD_KM)
          .withColumn("collision_type", lit("SAT-DEB"))
          .drop("bucket")
          .dropDuplicates("norad_1", "norad_2")

        allCollisions = if (allCollisions.isEmpty) satDeb else allCollisions.union(satDeb)
        println(s"    SAT-DEB close approaches: ${satDeb.count()}")
      }

      // ============================================================
      // 7. RISK CLASSIFICATION + PROBABILITY
      // ============================================================
      println("\n[6/7] Classifying risk levels...")

      if (!allCollisions.isEmpty) {
        val collisionResults = allCollisions
          // Risk classification (reference thresholds)
          .withColumn("risk_level",
            when(col("distance_km") <= CRITICAL_THRESHOLD_KM, "CRITICAL")
            .when(col("distance_km") <= HIGH_RISK_THRESHOLD_KM, "HIGH")
            .when(col("distance_km") <= MEDIUM_RISK_THRESHOLD_KM, "MEDIUM")
            .otherwise("LOW")
          )
          // Collision probability (inverse-distance, matching reference)
          .withColumn("collision_probability",
            when(col("distance_km") <= 0.01, lit(1.0))
            .otherwise(lit(1.0) / (lit(1.0) + col("distance_km") * col("distance_km")))
          )
          // Relative velocity (scalar approximation, matching reference)
          .withColumn("relative_velocity_kms",
            when(col("vel_1").isNotNull && col("vel_2").isNotNull,
              col("vel_1") + col("vel_2"))
            .otherwise(lit(null).cast(DoubleType))
          )
          // Detection timestamp
          .withColumn("detection_timestamp", current_timestamp())
          .withColumn("batch_id", lit(batchTimestamp))

        val totalCollisions = collisionResults.count()
        println(s"  Total collision pairs: $totalCollisions")

        // Risk distribution
        println("\n  Risk Level Distribution:")
        collisionResults.groupBy("risk_level").count().orderBy("risk_level").show()

        println("  Collision Type Distribution:")
        collisionResults.groupBy("collision_type").count().show()

        // Top 10 closest approaches
        println("  Top 10 Closest Approaches:")
        collisionResults
          .orderBy(col("distance_km").asc)
          .select("norad_1", "name_1", "norad_2", "name_2",
            "collision_type", "distance_km", "risk_level", "collision_probability")
          .show(10, truncate = false)

        // ============================================================
        // 8. SAVE RESULTS
        // ============================================================
        println("[7/7] Saving results...")

        // --- 8a. HDFS: Timestamped batch (like reference project) ---
        val hdfsOutputPath = s"$HDFS_COLLISION_OUTPUT/batch_$batchTimestamp"
        collisionResults
          .write.mode("overwrite")
          .parquet(hdfsOutputPath)
        println(s"  HDFS: Saved to $hdfsOutputPath")

        // --- 8b. HDFS: High-risk alerts separately ---
        val highRiskAlerts = collisionResults
          .filter(col("risk_level").isin("CRITICAL", "HIGH"))
        val highRiskCount = highRiskAlerts.count()

        if (highRiskCount > 0) {
          highRiskAlerts
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(s"$HDFS_COLLISION_OUTPUT/high_risk_batch_$batchTimestamp")
          println(s"  HDFS: $highRiskCount HIGH/CRITICAL alerts saved")
        }

        // --- 8c. Kafka: Publish collision alerts (like reference project) ---
        try {
          println("  Publishing alerts to Kafka...")
          val kafkaDF = collisionResults
            .selectExpr("CAST(norad_1 AS STRING) as key", "to_json(struct(*)) as value")

          kafkaDF.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", KAFKA_COLLISION_TOPIC)
            .save()

          println(s"  Kafka: Published $totalCollisions alerts to topic '$KAFKA_COLLISION_TOPIC'")
        } catch {
          case e: Exception =>
            println(s"  Kafka: Could not publish (${e.getMessage}) - continuing without Kafka")
        }

        // --- 8d. Save pipeline metrics to HDFS ---
        val pipelineDuration = (System.currentTimeMillis() - pipelineStart) / 1000.0
        val metricsDF = Seq((
          batchTimestamp,
          totalCollisions,
          highRiskCount,
          activeCount,
          satCount,
          debCount,
          stoppedCount,
          pipelineDuration,
          java.time.LocalDateTime.now().toString
        )).toDF(
          "batch_id", "total_collisions", "high_risk_count",
          "active_objects", "satellites", "debris",
          "stopped_objects", "pipeline_duration_sec", "completed_at"
        )
        metricsDF.coalesce(1).write.mode("append")
          .parquet(s"$HDFS_COLLISION_OUTPUT/pipeline_metrics")
        println(s"  Metrics saved to HDFS")

        // ============================================================
        // SUMMARY
        // ============================================================
        val pipelineEnd = (System.currentTimeMillis() - pipelineStart) / 1000.0
        println("\n" + "=" * 70)
        println("  COLLISION PREDICTION SUMMARY")
        println("=" * 70)
        println(f"""
          |  Batch ID:             $batchTimestamp
          |  Pipeline Duration:    $pipelineEnd%.1f seconds
          |
          |  Objects Analyzed:     $activeCount
          |  Satellites:           $satCount
          |  Debris:               $debCount
          |  Stopped Tracking:     $stoppedCount
          |
          |  Collision Pairs:      $totalCollisions
          |  High Risk Alerts:     $highRiskCount
          |
          |  Thresholds:
          |    CRITICAL:  <= $CRITICAL_THRESHOLD_KM%.1f km
          |    HIGH:      <= $HIGH_RISK_THRESHOLD_KM%.1f km
          |    MEDIUM:    <= $MEDIUM_RISK_THRESHOLD_KM%.1f km
          |    LOW:       <= $COLLISION_THRESHOLD_KM%.1f km
          |
          |  Outputs:
          |    HDFS:  $hdfsOutputPath
          |    Kafka: $KAFKA_COLLISION_TOPIC
          |""".stripMargin)

      } else {
        println("  No collisions detected within threshold.")
        println("\n[7/7] No results to save.")
      }

      // Unpersist cached DataFrames
      activeObjects.unpersist()
      activeWithShell.unpersist()
      activeWithML.unpersist()
      satellites.unpersist()
      debris.unpersist()

      println("=" * 70)
      println("  Pipeline completed successfully.")
      println("=" * 70)

    } catch {
      case e: Exception =>
        println(s"\n  Pipeline FAILED: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
