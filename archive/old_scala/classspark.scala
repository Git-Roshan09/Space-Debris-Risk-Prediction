import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object classspark extends App {
    
    val spark = SparkSession.builder()
      .appName("SalesAnalysis")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    val sales = sc.parallelize(List(1200, 900, 1500, 700, 1800))

    val totalSales = sales.sum()
    val daysCount = sales.count()
    val firstDaySale = sales.first()

    println(s"Start Data: ${sales.collect().mkString(", ")}")
    println(s"Total Sales: $totalSales")
    println(s"Days Recorded: $daysCount")
    println(s"First Day Sale: $firstDaySale")
    
 
}