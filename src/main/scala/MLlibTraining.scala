import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MLlibTraining extends App {

  // ============================================================
  // SPARK INITIALIZATION
  // ============================================================
  val spark = SparkSession.builder()
    .appName("SpaceDebris-MLlib-Training")
    .master("local[12]")
    .config("spark.driver.memory", "8g")
    .config("spark.hadoop.dfs.replication", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  println("=" * 60)
  println("  SPACE DEBRIS MLlib TRAINING PIPELINE")
  println("=" * 60)

  // ============================================================
  // PART 1: LINEAR REGRESSION - POSITION PREDICTION
  // ============================================================
  println("\n" + "=" * 60)
  println("  PART 1: LINEAR REGRESSION - TRAJECTORY PREDICTION")
  println("=" * 60)

  // Load state vectors from HDFS (Parquet format)
  // Sample 500K records for practical ML training (full dataset is 31+ GB)
  val allStateVectors = spark.read
    .parquet("hdfs://localhost:9000/space-debris/state-vectors")
    .select(
      col("EPOCH"),
      col("TLE_LINE1"),
      col("TLE_LINE2"),
      col("POS_X"), col("POS_Y"), col("POS_Z"),
      col("VEL_X"), col("VEL_Y"), col("VEL_Z")
    )
    .na.drop()

  val stateVectorsDF = allStateVectors.sample(false, 0.01, seed = 42).limit(500000)
  println(s"\nSampled state vector records for training: ${stateVectorsDF.count()}")
  println("\nSample data:")
  stateVectorsDF.select("EPOCH", "POS_X", "POS_Y", "POS_Z", "VEL_X", "VEL_Y", "VEL_Z").show(5, truncate = false)

  // Add computed features
  val featuredDF = stateVectorsDF
    .withColumn("ALTITUDE", sqrt(col("POS_X") * col("POS_X") + col("POS_Y") * col("POS_Y") + col("POS_Z") * col("POS_Z")))
    .withColumn("SPEED", sqrt(col("VEL_X") * col("VEL_X") + col("VEL_Y") * col("VEL_Y") + col("VEL_Z") * col("VEL_Z")))

  println("\nComputed Features (Altitude & Speed):")
  featuredDF.select("ALTITUDE", "SPEED").describe().show()

  // Create sliding window: use current state to predict next POS_X
  // Add row number per object to create sequential ordering
  val windowedDF = featuredDF
    .withColumn("row_num", monotonically_increasing_id())
    .orderBy("row_num")

  // Use current velocity and position to predict next position (simple regression)
  // Features: [POS_X, POS_Y, POS_Z, VEL_X, VEL_Y, VEL_Z]
  // Label: ALTITUDE (predict orbital altitude)
  val assembler = new VectorAssembler()
    .setInputCols(Array("POS_X", "POS_Y", "POS_Z", "VEL_X", "VEL_Y", "VEL_Z"))
    .setOutputCol("features")

  val assembledDF = assembler.transform(windowedDF)

  // Split data: 80% train, 20% test
  val Array(trainingData, testData) = assembledDF.randomSplit(Array(0.8, 0.2), seed = 42)

  println(s"\nTraining set size: ${trainingData.count()}")
  println(s"Test set size: ${testData.count()}")

  // --- Model 1: Predict ALTITUDE from position + velocity ---
  println("\n--- Model 1: Predicting Orbital Altitude ---")
  val lrAltitude = new LinearRegression()
    .setLabelCol("ALTITUDE")
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setRegParam(0.01)
    .setElasticNetParam(0.5)

  val altitudeModel = lrAltitude.fit(trainingData)

  // Print model coefficients
  println(s"Coefficients: ${altitudeModel.coefficients}")
  println(s"Intercept: ${altitudeModel.intercept}")

  // Training summary
  val altitudeSummary = altitudeModel.summary
  println(s"RMSE (Training): ${altitudeSummary.rootMeanSquaredError}")
  println(s"R2 (Training): ${altitudeSummary.r2}")

  // Test evaluation
  val altitudePredictions = altitudeModel.transform(testData)
  val altitudeEvaluator = new RegressionEvaluator()
    .setLabelCol("ALTITUDE")
    .setPredictionCol("prediction")
    .setMetricName("rmse")

  val altitudeRMSE = altitudeEvaluator.evaluate(altitudePredictions)
  val altitudeR2Evaluator = new RegressionEvaluator()
    .setLabelCol("ALTITUDE")
    .setPredictionCol("prediction")
    .setMetricName("r2")
  val altitudeR2 = altitudeR2Evaluator.evaluate(altitudePredictions)

  println(s"\nRMSE (Test): $altitudeRMSE km")
  println(s"R2 (Test):   $altitudeR2")

  // Show predictions vs actual
  println("\nPredictions vs Actual (Altitude):")
  altitudePredictions.select("POS_X", "POS_Y", "POS_Z", "ALTITUDE", "prediction")
    .withColumn("error_km", abs(col("ALTITUDE") - col("prediction")))
    .show(10, truncate = false)

  // --- Model 2: Predict SPEED from position ---
  println("\n--- Model 2: Predicting Orbital Speed ---")
  val posAssembler = new VectorAssembler()
    .setInputCols(Array("POS_X", "POS_Y", "POS_Z"))
    .setOutputCol("pos_features")

  val posAssembledDF = posAssembler.transform(windowedDF)
  val Array(trainSpeed, testSpeed) = posAssembledDF.randomSplit(Array(0.8, 0.2), seed = 42)

  val lrSpeed = new LinearRegression()
    .setLabelCol("SPEED")
    .setFeaturesCol("pos_features")
    .setMaxIter(100)
    .setRegParam(0.01)

  val speedModel = lrSpeed.fit(trainSpeed)
  val speedPredictions = speedModel.transform(testSpeed)

  val speedEvaluator = new RegressionEvaluator()
    .setLabelCol("SPEED")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val speedRMSE = speedEvaluator.evaluate(speedPredictions)

  val speedR2Evaluator = new RegressionEvaluator()
    .setLabelCol("SPEED")
    .setPredictionCol("prediction")
    .setMetricName("r2")
  val speedR2 = speedR2Evaluator.evaluate(speedPredictions)

  println(s"RMSE (Test): $speedRMSE km/s")
  println(s"R2 (Test):   $speedR2")

  speedPredictions.select("POS_X", "POS_Y", "POS_Z", "SPEED", "prediction")
    .withColumn("error_km_s", abs(col("SPEED") - col("prediction")))
    .show(10, truncate = false)

  // --- SAVE TRAJECTORY MODELS ---
  println("\nSaving Trajectory Models to HDFS...")
  altitudeModel.write.overwrite().save("hdfs://localhost:9000/space-debris/models/trajectory-altitude")
  speedModel.write.overwrite().save("hdfs://localhost:9000/space-debris/models/trajectory-speed")
  println("✅ Models saved: altitude and speed.")

  // ============================================================
  // PART 2: K-MEANS CLUSTERING - ORBIT CLASSIFICATION
  // ============================================================
  println("\n" + "=" * 60)
  println("  PART 2: K-MEANS CLUSTERING - ORBIT CLASSIFICATION")
  println("=" * 60)

  // Load catalog data from HDFS (Parquet)
  val catalogDF = spark.read
    .parquet("hdfs://localhost:9000/space-debris/catalog")
    .na.drop(Seq("PERIOD", "INCLINATION", "APOGEE", "PERIGEE"))
    .filter(col("PERIOD") > 0 && col("APOGEE") > 0 && col("PERIGEE") > 0)

  println(s"\nCatalog records loaded: ${catalogDF.count()}")
  println("\nOrbital statistics:")
  catalogDF.select("PERIOD", "INCLINATION", "APOGEE", "PERIGEE").describe().show()

  // Features for clustering: orbital characteristics
  val orbitAssembler = new VectorAssembler()
    .setInputCols(Array("PERIOD", "INCLINATION", "APOGEE", "PERIGEE"))
    .setOutputCol("orbit_features")

  val clusterInputDF = orbitAssembler.transform(catalogDF)

  // K-Means with K=4 (LEO, MEO, GEO, HEO)
  val kmeans = new KMeans()
    .setK(4)
    .setSeed(42)
    .setFeaturesCol("orbit_features")
    .setPredictionCol("orbit_cluster")
    .setMaxIter(50)

  val kmeansModel = kmeans.fit(clusterInputDF)

  // Print cluster centers
  println("\nCluster Centers:")
  println(f"${"Cluster"}%-10s ${"Period(min)"}%-15s ${"Inclination(°)"}%-18s ${"Apogee(km)"}%-15s ${"Perigee(km)"}%-15s ${"Orbit Type"}%-10s")
  println("-" * 83)

  kmeansModel.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
    val period = center(0)
    val inclination = center(1)
    val apogee = center(2)
    val perigee = center(3)

    // Classify orbit type based on altitude
    val orbitType = if (perigee < 2000) "LEO"
                    else if (perigee < 20000) "MEO"
                    else if (perigee > 35000) "GEO"
                    else "HEO"

    println(f"$idx%-10d $period%-15.2f $inclination%-18.2f $apogee%-15.2f $perigee%-15.2f $orbitType%-10s")
  }

  // Add cluster predictions
  val clusteredDF = kmeansModel.transform(clusterInputDF)

  // Count objects per cluster
  println("\nObjects per cluster:")
  clusteredDF.groupBy("orbit_cluster")
    .agg(
      count("*").as("count"),
      avg("APOGEE").as("avg_apogee"),
      avg("PERIGEE").as("avg_perigee"),
      avg("PERIOD").as("avg_period")
    )
    .orderBy("orbit_cluster")
    .show()

  // Country distribution per cluster
  println("\nTop countries per cluster:")
  clusteredDF.groupBy("orbit_cluster", "COUNTRY")
    .count()
    .orderBy(col("orbit_cluster"), col("count").desc)
    .show(20)

  // --- SAVE CLUSTERING MODEL ---
  println("\nSaving Clustering Model to HDFS...")
  kmeansModel.write.overwrite().save("hdfs://localhost:9000/space-debris/models/orbit-clustering")
  println("✅ Clustering model saved.")

  // ============================================================
  // PART 3: RANDOM FOREST - DEBRIS CLASSIFICATION
  // ============================================================
  println("\n" + "=" * 60)
  println("  PART 3: RANDOM FOREST - OBJECT TYPE CLASSIFICATION")
  println("=" * 60)

  // Prepare data with labels for supervised learning
  val labeledCatalog = catalogDF
    .filter(col("OBJECT_TYPE").isNotNull)
    .withColumn("LABEL_NAME", when(col("OBJECT_TYPE") === "DEBRIS", "DEBRIS").otherwise("SATELLITE"))

  // Index labels (DEBRIS -> 0, SATELLITE -> 1 etc)
  val labelIndexer = new StringIndexer()
    .setInputCol("LABEL_NAME")
    .setOutputCol("label")
    .fit(labeledCatalog)

  // Features for classification
  val classifierAssembler = new VectorAssembler()
    .setInputCols(Array("PERIOD", "INCLINATION", "APOGEE", "PERIGEE"))
    .setOutputCol("features")

  val classifierInput = classifierAssembler.transform(labelIndexer.transform(labeledCatalog))

  // Split data
  val Array(classTrain, classTest) = classifierInput.randomSplit(Array(0.8, 0.2))

  // Train Random Forest Classifier
  val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(20)

  println("Training Random Forest Classifier...")
  val rfModel = rf.fit(classTrain)

  // Evaluate
  val classPredictions = rfModel.transform(classTest)
  val classEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val classAccuracy = classEvaluator.evaluate(classPredictions)
  println(f"✅ Random Forest Accuracy: ${classAccuracy * 100}%.2f%%")

  // --- SAVE CLASSIFIER MODELS ---
  println("\nSaving Classifier Models to HDFS...")
  rfModel.write.overwrite().save("hdfs://localhost:9000/space-debris/models/debris-classifier")
  labelIndexer.write.overwrite().save("hdfs://localhost:9000/space-debris/models/classifier-label-indexer")
  println("✅ Classification models saved.")

  // ============================================================
  // PART 4: SUMMARY
  // ============================================================
  println("\n" + "=" * 60)
  println("  RESULTS SUMMARY")
  println("=" * 60)
  println(f"""
    |  LINEAR REGRESSION RESULTS:
    |  ─────────────────────────
    |  Altitude Prediction:
    |    RMSE:  $altitudeRMSE%.4f km
    |    R²:    $altitudeR2%.4f
    |
    |  Speed Prediction:
    |    RMSE:  $speedRMSE%.4f km/s  
    |    R²:    $speedR2%.4f
    |
    |  K-MEANS CLUSTERING RESULTS:
    |  ───────────────────────────
    |  Clusters: ${kmeansModel.clusterCenters.length}
    |  Total debris classified: ${clusteredDF.count()}
    |
    """.stripMargin)

  // Save clustered data to HDFS
  clusteredDF
    .select("NORAD_CAT_ID", "OBJECT_NAME", "COUNTRY", "PERIOD", "INCLINATION", "APOGEE", "PERIGEE", "orbit_cluster")
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("hdfs://localhost:9000/space-debris/ml-results/clustered-debris")

  println("  Clustered data saved to HDFS: /space-debris/ml-results/clustered-debris/")
  println("=" * 60)

  // --- SAVE METRICS SUMMARY TO HDFS ---
  println("\nSaving Metrics Summary to HDFS...")
  val metricsMap = Map(
    "altitude_rmse" -> altitudeRMSE,
    "altitude_r2" -> altitudeR2,
    "speed_rmse" -> speedRMSE,
    "speed_r2" -> speedR2,
    "classification_accuracy" -> classAccuracy,
    "training_timestamp" -> java.time.LocalDateTime.now().toString
  )

  case class MetricsRow(
    altitude_rmse: Double,
    altitude_r2: Double,
    speed_rmse: Double,
    speed_r2: Double,
    classification_accuracy: Double,
    training_timestamp: String
  )
  import spark.implicits._
  val metricsDF = Seq(MetricsRow(
    altitude_rmse            = altitudeRMSE,
    altitude_r2              = altitudeR2,
    speed_rmse               = speedRMSE,
    speed_r2                 = speedR2,
    classification_accuracy  = classAccuracy,
    training_timestamp       = java.time.LocalDateTime.now().toString
  )).toDF()
  metricsDF.write.mode("overwrite").json("hdfs://localhost:9000/space-debris/ml-results/metrics")
  println("✅ Metrics saved to /space-debris/ml-results/metrics")

  spark.stop()
}
