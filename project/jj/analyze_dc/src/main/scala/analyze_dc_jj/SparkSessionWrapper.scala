package analyze_dc_jj

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val conf = new SparkConf()
    .set("spark.driver.memory", "1g")
    .set("spark.excutor.memory", "2g")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/Users/user/Documents/study/ss-spark/project/jj/analyze_dc/log")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .master("local")
      .appName("AnalyzeDC")
      .getOrCreate()
  }
}
