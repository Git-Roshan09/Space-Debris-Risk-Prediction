import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.io.File
import org.orekit.data.DataContext
import org.orekit.data.DirectoryCrawler
import org.orekit.propagation.analytical.tle.TLE
import org.orekit.propagation.analytical.tle.TLEPropagator
import org.orekit.time.AbsoluteDate
import org.orekit.time.TimeScalesFactory
import org.orekit.frames.FramesFactory
import scala.io.Source
import java.net.{HttpURLConnection, URL}
import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}

object TLEProcessor {

  // WebHDFS Configuration (uses REST API, works from outside Docker)
  val HDFS_NAMENODE = sys.env.getOrElse("HDFS_NAMENODE", "localhost")
  val WEBHDFS_PORT = sys.env.getOrElse("WEBHDFS_PORT", "9870")
  val HDFS_USER = sys.env.getOrElse("HDFS_USER", "root")
  val HDFS_BASE_PATH = "/space-debris-webhdfs"
  
  // Local catalog path (already downloaded)
  val LOCAL_CATALOG_PATH = "Output/space_debris_catalog.csv"
  
  // Processing Configuration
  val START_INDEX = sys.env.getOrElse("START_INDEX", "976").toInt
  val N_DEBRIS = sys.env.getOrElse("N_DEBRIS", "216").toInt

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("🚀 TLE BATCH PROCESSOR (Local Catalog + WebHDFS)")
    println("=" * 70)
    println(s"📥 Catalog:     $LOCAL_CATALOG_PATH (local)")
    println(s"📥 TLE Input:   WebHDFS at $HDFS_NAMENODE:$WEBHDFS_PORT$HDFS_BASE_PATH/tle-history/")
    println(s"📤 Output:      WebHDFS at $HDFS_NAMENODE:$WEBHDFS_PORT$HDFS_BASE_PATH/state-vectors/")
    println(s"📊 Start Index: $START_INDEX")
    println(s"📊 Max Items:   $N_DEBRIS")
    println("=" * 70 + "\n")

    // 1. Initialize Orekit
    val orekitData = new File("orekit-data")
    if (!orekitData.exists()) {
      println("❌ Orekit data folder not found!")
      System.exit(1)
    }
    val manager = DataContext.getDefault().getDataProvidersManager()
    manager.addProvider(new DirectoryCrawler(orekitData))
    println("✅ Orekit initialized")

    // 2. Create Spark Session (local mode, no HDFS dependency)
    val spark = SparkSession.builder()
      .appName("TLE State Vector Processor")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("✅ Spark session created")

    // 3. Read local catalog
    println("\n📖 Reading debris catalog from local file...")
    val catalogFile = new File(LOCAL_CATALOG_PATH)
    if (!catalogFile.exists()) {
      println(s"❌ Catalog not found at $LOCAL_CATALOG_PATH")
      println("   Run 'python debris.py' first to download the catalog")
      spark.stop()
      System.exit(1)
    }
    
    val catalogDF = spark.read
      .option("header", "true")
      .csv(LOCAL_CATALOG_PATH)
    
    // Get NORAD IDs in catalog order (row by row)
    val allNoradIds = catalogDF
      .select("NORAD_CAT_ID")
      .filter(col("NORAD_CAT_ID").isNotNull)
      .collect()
      .map(_.getString(0))
    
    val noradIds = allNoradIds.slice(START_INDEX, START_INDEX + N_DEBRIS)
    
    println(s"   Found ${noradIds.length} NORAD IDs to process\n")

    // TLE Schema
    val tleSchema = new StructType()
      .add("EPOCH", StringType, true)
      .add("TLE_LINE1", StringType, true)
      .add("TLE_LINE2", StringType, true)

    // UDF for SGP4 State Vector calculation
    // Uses the TLE's internal epoch for propagation (more reliable than parsing external epoch)
    val calculateStateVector = udf((line1: String, line2: String, epochStr: String) => {
      try {
        val tle = new TLE(line1, line2)
        val propagator = TLEPropagator.selectExtrapolator(tle)
        
        // Use TLE's own epoch date for propagation (embedded in the TLE data)
        val tleDate = tle.getDate()
        val pvCoordinates = propagator.getPVCoordinates(tleDate, FramesFactory.getGCRF)
        
        val pos = pvCoordinates.getPosition
        val vel = pvCoordinates.getVelocity
        
        // Return position (m -> km) and velocity (m/s -> km/s)
        Array(
          pos.getX / 1000.0, pos.getY / 1000.0, pos.getZ / 1000.0,
          vel.getX / 1000.0, vel.getY / 1000.0, vel.getZ / 1000.0
        )
      } catch {
        case e: Exception => 
          // println(s"Error: ${e.getMessage}")  // Uncomment for debugging
          Array.fill(6)(Double.NaN)
      }
    })

    // 4. Process each NORAD ID
    var processedCount = 0
    var skippedCount = 0
    var errorCount = 0

    for ((noradId, idx) <- noradIds.zipWithIndex) {
      val currentIdx = START_INDEX + idx + 1
      val inputPath = s"$HDFS_BASE_PATH/tle-history/${noradId}_tle.csv"
      val outputPath = s"$HDFS_BASE_PATH/state-vectors/${noradId}_state_vectors.csv"
      
      print(s"[${currentIdx}/${START_INDEX + noradIds.length}] NORAD $noradId: ")
      
      try {
        // Check if output already exists
        if (checkWebHDFSFileExists(outputPath)) {
          println("⏭️  Already processed, skipping")
          skippedCount += 1
        } else {
          // Read TLE from WebHDFS
          val tleData = readFromWebHDFS(inputPath)
          
          if (tleData.isEmpty) {
            println("⚠️  TLE file not found or empty")
            skippedCount += 1
          } else {
            // Parse CSV data
            val lines = tleData.split("\n").filter(_.nonEmpty)
            if (lines.length <= 1) {
              println("⚠️  No TLE records")
              skippedCount += 1
            } else {
              val header = lines.head.split(",").map(_.trim.replace("\"", ""))
              val epochIdx = header.indexOf("EPOCH")
              val line1Idx = header.indexOf("TLE_LINE1")
              val line2Idx = header.indexOf("TLE_LINE2")
              
              val records = lines.tail.map { line =>
                val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim.replace("\"", ""))
                (cols.lift(epochIdx).getOrElse(""), cols.lift(line1Idx).getOrElse(""), cols.lift(line2Idx).getOrElse(""))
              }.filter(r => r._1.nonEmpty && r._2.nonEmpty && r._3.nonEmpty)
              
              // Process with Spark
              val tleDF = records.toSeq.toDF("EPOCH", "TLE_LINE1", "TLE_LINE2")
              
              val processedDF = tleDF
                .withColumn("NORAD_ID", lit(noradId))
                .withColumn("sv", calculateStateVector($"TLE_LINE1", $"TLE_LINE2", $"EPOCH"))
                .select(
                  $"NORAD_ID", $"EPOCH",
                  $"sv"(0).as("POS_X"), $"sv"(1).as("POS_Y"), $"sv"(2).as("POS_Z"),
                  $"sv"(3).as("VEL_X"), $"sv"(4).as("VEL_Y"), $"sv"(5).as("VEL_Z")
                )
                .filter(!isnan($"POS_X"))
              
              // Write to local temp file then upload to HDFS
              val outputCSV = processedDF.collect().map { row =>
                s"${row.getString(0)},${row.getString(1)},${row.getDouble(2)},${row.getDouble(3)},${row.getDouble(4)},${row.getDouble(5)},${row.getDouble(6)},${row.getDouble(7)}"
              }
              
              val csvContent = "NORAD_ID,EPOCH,POS_X,POS_Y,POS_Z,VEL_X,VEL_Y,VEL_Z\n" + outputCSV.mkString("\n")
              
              if (writeToWebHDFS(outputPath, csvContent)) {
                println(s"✅ ${records.length} TLEs → ${outputCSV.length} state vectors")
                processedCount += 1
              } else {
                println("❌ Failed to write to HDFS")
                errorCount += 1
              }
            }
          }
        }
      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage.take(50)}")
          errorCount += 1
      }
    }

    // Summary
    println("\n" + "=" * 70)
    println("🎯 PROCESSING COMPLETE")
    println("=" * 70)
    println(s"   ✅ Processed: $processedCount")
    println(s"   ⏭️  Skipped:   $skippedCount")
    println(s"   ❌ Errors:    $errorCount")
    println("=" * 70)
    
    if (START_INDEX + noradIds.length < catalogDF.count()) {
      println(s"\n💡 To continue: START_INDEX=${START_INDEX + noradIds.length} sbt run")
    }

    spark.stop()
  }

  // WebHDFS Helper: Check if file exists
  def checkWebHDFSFileExists(path: String): Boolean = {
    try {
      val url = new URL(s"http://$HDFS_NAMENODE:$WEBHDFS_PORT/webhdfs/v1$path?op=GETFILESTATUS&user.name=$HDFS_USER")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.getResponseCode == 200
    } catch {
      case _: Exception => false
    }
  }

  // WebHDFS Helper: Read file content
  def readFromWebHDFS(path: String): String = {
    try {
      val url = new URL(s"http://$HDFS_NAMENODE:$WEBHDFS_PORT/webhdfs/v1$path?op=OPEN&user.name=$HDFS_USER")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setInstanceFollowRedirects(false)
      
      if (conn.getResponseCode == 307) {
        var redirectUrl = conn.getHeaderField("Location")
        redirectUrl = redirectUrl.replace("datanode:9864", s"$HDFS_NAMENODE:9864")
        
        val dataConn = new URL(redirectUrl).openConnection().asInstanceOf[HttpURLConnection]
        val reader = new BufferedReader(new InputStreamReader(dataConn.getInputStream))
        val content = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        reader.close()
        content
      } else ""
    } catch {
      case _: Exception => ""
    }
  }

  // WebHDFS Helper: Write file content
  def writeToWebHDFS(path: String, content: String): Boolean = {
    try {
      // Create directory
      val mkdirUrl = new URL(s"http://$HDFS_NAMENODE:$WEBHDFS_PORT/webhdfs/v1${path.substring(0, path.lastIndexOf('/'))}?op=MKDIRS&user.name=$HDFS_USER")
      val mkdirConn = mkdirUrl.openConnection().asInstanceOf[HttpURLConnection]
      mkdirConn.setRequestMethod("PUT")
      mkdirConn.getResponseCode
      
      // Create file
      val createUrl = new URL(s"http://$HDFS_NAMENODE:$WEBHDFS_PORT/webhdfs/v1$path?op=CREATE&overwrite=true&replication=1&user.name=$HDFS_USER")
      val createConn = createUrl.openConnection().asInstanceOf[HttpURLConnection]
      createConn.setRequestMethod("PUT")
      createConn.setInstanceFollowRedirects(false)
      
      if (createConn.getResponseCode == 307) {
        var redirectUrl = createConn.getHeaderField("Location")
        redirectUrl = redirectUrl.replace("datanode:9864", s"$HDFS_NAMENODE:9864")
        
        val dataConn = new URL(redirectUrl).openConnection().asInstanceOf[HttpURLConnection]
        dataConn.setRequestMethod("PUT")
        dataConn.setDoOutput(true)
        dataConn.setRequestProperty("Content-Type", "application/octet-stream")
        
        val writer = new OutputStreamWriter(dataConn.getOutputStream)
        writer.write(content)
        writer.close()
        
        dataConn.getResponseCode == 201
      } else false
    } catch {
      case _: Exception => false
    }
  }
}
