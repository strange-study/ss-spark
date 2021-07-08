
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, IDF, Tokenizer}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util

object DcTestApp {
  val ROOT_DATA_PATH = "data/output/"
  val BOARD_IDS = "board_ids.csv"
  val TARGET_DATE = "20210702/"

  val INPUT_TYPE = ".csv"

  val spark: SparkSession = SparkSession.builder.appName("sparkTest")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val boardIdsData: DataFrame = getDataFrame(ROOT_DATA_PATH + BOARD_IDS)

  def main(args: Array[String]) {
    dcTest()
  }

  def testSample(): Unit = {
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "이거 게임 정말 좋아 정말 최고야"),
      (1.0, "이거 게임 별로임")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("token")

    val tf = new CountVectorizer()
      .setInputCol("token")
      .setOutputCol("tf")

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tf-idf")

    val pipe_tf = new Pipeline()
      .setStages(Array(tokenizer, tf, idf))
      .fit(sentenceData)

    val transformed = pipe_tf.transform(sentenceData)
    transformed.drop("label", "sentence").createOrReplaceTempView("tf")
  }

  def dcTest(): Unit = {
    val inputSchema = new StructType()
      .add("ID", StringType)
      .add("CONTENTS", ArrayType(StringType))
    val boardIds = boardIdsData.collect()
    val rows = new util.ArrayList[Row]()
    boardIds.foreach { row =>
      val boardId = row.mkString
      val contentsRow = getDataFrame(ROOT_DATA_PATH + TARGET_DATE + boardId + INPUT_TYPE)
        .select("title")
        .collect()
        .map(row => row.mkString)
      rows.add(Row(boardId, contentsRow))
    }
    val inputDf = spark.createDataFrame(rows, inputSchema)
    inputDf.show()
  }//겔러리 [id, title array] df 반환

  def createCsvFileFromDataframe(dataFrame: DataFrame, dirName: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(ROOT_DATA_PATH + dirName)
  }

  def getDataFrame(path: String): DataFrame = {
    spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
  }


}