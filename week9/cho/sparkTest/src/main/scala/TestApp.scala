import org.apache.spark.sql.{DataFrame, SparkSession}

object TestApp {
  val SCORE_SUFFIX = "score"
  val TARGET_NAME = "math score"

  val spark: SparkSession = SparkSession.builder.appName("sparkTest")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val studentsPerformanceData: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/StudentsPerformance.csv")

  val columnNames: Array[String] = studentsPerformanceData
    .columns
    .filter(p => {
      !p.contains("score")
    })

  def main(args: Array[String]) {
    for (columnName <- columnNames) {
      createCsvFileFromDataframe(
        calculateMeanTargetGroupByKey(columnName, TARGET_NAME),
        columnName
      )
    }

    spark.stop()
  }

  def calculateMeanTargetGroupByKey(groupKey: String, target: String): DataFrame = {
    studentsPerformanceData
      .groupBy(groupKey)
      .mean(target)
  }

  def createCsvFileFromDataframe(dataFrame: DataFrame, dirName: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save("data/" + dirName)
  }

}