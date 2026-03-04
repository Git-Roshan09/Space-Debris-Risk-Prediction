import org.apache.spark.sql.SparkSession

object tryspark extends App {
    val spark = SparkSession.builder()
      .appName("TrySpark")
      .master("local[*]")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR") 
      
    val sc = spark.sparkContext
    val marks = sc.parallelize(List(45,67,89,23,90,56,58,2,11))
    val passed = marks.filter(_>=50)
    val passCount = passed.count()
    val passedMarks = passed.collect()

    println(s"Number of students passed: $passCount")
    println(s"Marks of passed students: ${passedMarks.mkString(", ")}")

}