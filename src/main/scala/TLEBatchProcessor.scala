import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import java.io.File
import org.orekit.data.DataContext
import org.orekit.data.DirectoryCrawler
import org.orekit.propagation.analytical.tle.{TLE => OrekitTLE}
import org.orekit.propagation.analytical.tle.TLEPropagator
import org.orekit.frames.FramesFactory

// TLE Batch Processor - Reads raw TLE text files, computes SGP4 state vectors, writes to HDFS as Parquet.
// Input:  data/ directory (.txt files, raw two-line TLE format from Space-Track)
// Output: HDFS /space-debris/state-vectors/ (Parquet)
object TLEBatchProcessor {

  val DATA_DIR = sys.env.getOrElse("DATA_DIR", "data1")
  val HDFS_BASE_PATH = "/space-debris"

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("  TLE BATCH PROCESSOR (Local TXT -> SGP4 -> HDFS Parquet)")
    println("=" * 70)

    // 1. Initialize Orekit
    val orekitData = new File("orekit-data")
    if (!orekitData.exists()) {
      println("Orekit data folder not found!")
      System.exit(1)
    }
    val manager = DataContext.getDefault().getDataProvidersManager()
    manager.addProvider(new DirectoryCrawler(orekitData))
    println("Orekit initialized")

    // 2. Spark Session
    val spark = SparkSession.builder()
      .appName("TLE-Batch-Processor")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.hadoop.dfs.replication", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    println("Spark session created\n")

    // 3. Discover TLE files
    val dataDir = new File(DATA_DIR)
    if (!dataDir.exists() || !dataDir.isDirectory) {
      println(s"Data directory not found: $DATA_DIR")
      spark.stop(); System.exit(1)
    }

    val tleFiles = dataDir.listFiles()
      .filter(f => f.getName.endsWith(".txt") && f.length() > 0)
      .sortBy(_.getName)

    println(s"Found ${tleFiles.length} TLE files:")
    tleFiles.foreach(f => println(f"  ${f.getName}%-30s (${f.length() / (1024 * 1024)}%,d MB)"))

    // 4. Process each file
    var totalProcessed = 0L
    var totalPairs = 0L
    var fileCount = 0

    for (tleFile <- tleFiles) {
      fileCount += 1
      println(s"\n${"=" * 70}")
      println(s"[$fileCount/${tleFiles.length}] Processing: ${tleFile.getName}")
      println(s"${"=" * 70}")

      try {
        // Read file and pair TLE lines using mapPartitions (fully distributed, no collect)
        println("  Parsing TLE pairs...")

        val pairedRDD = spark.sparkContext.textFile(tleFile.getAbsolutePath)
          .map(_.replaceAll("""\\+\s*$""", "").trim)
          .filter(_.nonEmpty)
          .mapPartitions { lines =>
            // Within each partition, buffer Line1 and pair with next Line2
            var pendingLine1: String = null
            val pairs = scala.collection.mutable.ArrayBuffer[(Int, String, String)]()

            for (line <- lines) {
              if (line.startsWith("1 ") && line.length >= 60) {
                pendingLine1 = line
              } else if (line.startsWith("2 ") && line.length >= 60 && pendingLine1 != null) {
                // Validate NORAD ID match
                try {
                  val norad1 = pendingLine1.substring(2, 7).trim
                  val norad2 = line.substring(2, 7).trim
                  if (norad1 == norad2 && norad1.nonEmpty) {
                    pairs += ((norad1.toInt, pendingLine1, line))
                  }
                } catch {
                  case _: Exception => // skip bad lines
                }
                pendingLine1 = null
              }
            }
            pairs.iterator
          }

        val pairCount = pairedRDD.count()
        totalPairs += pairCount
        println(s"  Valid TLE pairs: ${"%,d".format(pairCount)}")

        if (pairCount == 0) {
          println("  No valid pairs, skipping")
        } else {
          // Convert to DataFrame
          val tlePairDF = pairedRDD.toDF("NORAD_ID", "TLE_LINE1", "TLE_LINE2")

          println("  Computing SGP4 state vectors...")

          // SGP4 UDF
          val computeSGP4 = udf((line1: String, line2: String) => {
            try {
              val tle = new OrekitTLE(line1, line2)
              val propagator = TLEPropagator.selectExtrapolator(tle)
              val tleDate = tle.getDate()
              val pv = propagator.getPVCoordinates(tleDate, FramesFactory.getGCRF)

              val pos = pv.getPosition
              val vel = pv.getVelocity

              val px = pos.getX / 1000.0; val py = pos.getY / 1000.0; val pz = pos.getZ / 1000.0
              val vx = vel.getX / 1000.0; val vy = vel.getY / 1000.0; val vz = vel.getZ / 1000.0

              val alt = math.sqrt(px*px + py*py + pz*pz) - 6371.0
              val spd = math.sqrt(vx*vx + vy*vy + vz*vz)

              Array(px, py, pz, vx, vy, vz, alt, spd)
            } catch {
              case _: Exception => Array.fill(8)(Double.NaN)
            }
          })

          // Parse epoch from TLE Line 1 (columns 18-32)
          val parseEpoch = udf((line1: String) => {
            try {
              val epochStr = line1.substring(18, 32).trim
              val year = epochStr.substring(0, 2).toInt
              val fullYear = if (year < 57) 2000 + year else 1900 + year
              val dayOfYear = epochStr.substring(2).toDouble
              val wholeDays = dayOfYear.toInt
              val fracDay = dayOfYear - wholeDays

              val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
              cal.set(java.util.Calendar.YEAR, fullYear)
              cal.set(java.util.Calendar.DAY_OF_YEAR, wholeDays)
              cal.set(java.util.Calendar.HOUR_OF_DAY, (fracDay * 24).toInt)
              cal.set(java.util.Calendar.MINUTE, ((fracDay * 24 * 60) % 60).toInt)
              cal.set(java.util.Calendar.SECOND, ((fracDay * 24 * 3600) % 60).toInt)

              val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
              fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
              fmt.format(cal.getTime)
            } catch {
              case _: Exception => null
            }
          })

          // Apply SGP4
          val processedDF = tlePairDF
            .withColumn("EPOCH", parseEpoch($"TLE_LINE1"))
            .withColumn("sgp4", computeSGP4($"TLE_LINE1", $"TLE_LINE2"))
            .filter(!isnan($"sgp4"(0)))
            .select(
              $"NORAD_ID", $"EPOCH",
              $"TLE_LINE1", $"TLE_LINE2",
              $"sgp4"(0).as("POS_X"), $"sgp4"(1).as("POS_Y"), $"sgp4"(2).as("POS_Z"),
              $"sgp4"(3).as("VEL_X"), $"sgp4"(4).as("VEL_Y"), $"sgp4"(5).as("VEL_Z"),
              $"sgp4"(6).as("ALTITUDE_KM"), $"sgp4"(7).as("SPEED_KMS")
            )
            .filter($"ALTITUDE_KM" > 0)

          // Write directly to HDFS — no cache/count to save memory
          val hdfsPath = s"hdfs://localhost:9000$HDFS_BASE_PATH/state-vectors"
          println(s"  Writing to HDFS...")

          processedDF
            .write
            .mode(SaveMode.Append)
            .parquet(hdfsPath)

          println(s"  Done! Written to HDFS.")
        }

      } catch {
        case e: Exception =>
          println(s"  Error processing ${tleFile.getName}: ${e.getMessage}")
      }
    }

    // 5. Upload catalog to HDFS
    println(s"\n${"=" * 70}")
    println("Uploading debris catalog to HDFS...")
    val catalogFile = new File("Output/space_debris_catalog.csv")
    if (catalogFile.exists()) {
      val catalogDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Output/space_debris_catalog.csv")

      catalogDF.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .parquet(s"hdfs://localhost:9000$HDFS_BASE_PATH/catalog")

      println(s"  Catalog uploaded: ${catalogDF.count()} records")
    }

    // 6. Summary
    println(s"\n${"=" * 70}")
    println("  BATCH PROCESSING COMPLETE")
    println(s"${"=" * 70}")
    println(s"  Files:          $fileCount")
    println(s"  TLE Pairs:      ${"%,d".format(totalPairs)}")
    println(s"  State Vectors:  ${"%,d".format(totalProcessed)}")
    println(s"  Output:         hdfs://localhost:9000$HDFS_BASE_PATH/state-vectors/ (Parquet)")
    println(s"${"=" * 70}")

    spark.stop()
  }
}
