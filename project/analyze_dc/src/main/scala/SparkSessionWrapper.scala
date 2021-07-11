import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val conf = new SparkConf()
    .set("spark.driver.memory", "1g")
    .set("spark.excutor.memory", "2g")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/Users/jinju/Documents/study/ss-spark/project/analyze_dc/log")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val spark:SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("AnalyzeDC")
      .getOrCreate()
  }
}
