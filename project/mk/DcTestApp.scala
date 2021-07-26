
import org.apache.spark.ml.feature.{CountVectorizer, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util
import scala.collection.mutable.ArrayBuffer

object DcTestApp {
  val INPUT_DATA_PATH = "data/input/"
  val OUTPUT_DATA_PATH = "data/output/"

  val BOARD_IDS = "board_ids.csv"
  val TARGET_DATE = "20210702/"
  val FILE_TYPE = ".csv"

  val GALL_ID = "GALL_ID"
  val REDUCE_TITLE = "REDUCE_TITLE"
  val TITLE_TOKEN_WORDS = "TITLE_TOKEN_WORDS"
  val TF = "TF"
  val TF_IDF = "TF-IDF"
  val BEST_WORDS = "BEST_WORDS"

  val OUTPUT_FILE_NAME = "result"

  val spark: SparkSession = SparkSession.builder.appName("sparkTest")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "192.168.0.33")
    .getOrCreate()

  val boardIdsData: DataFrame = getDataFrame(INPUT_DATA_PATH + BOARD_IDS)

  val NUM_TILE_WORDS = 66214
  val NUM_K = 1000

  def main(args: Array[String]) {
    executeSemanticAnalysis()
    getDataFrame(OUTPUT_DATA_PATH + "result").show()
  }


  def executeSemanticAnalysis(): Unit = {

    val inputDf = getDcGallInputDf()

    val tokenizer = new Tokenizer()
      .setInputCol(REDUCE_TITLE)
      .setOutputCol(TITLE_TOKEN_WORDS)
    val titleTokenWords = tokenizer.transform(inputDf)

    val tf = new CountVectorizer()
      .setInputCol(TITLE_TOKEN_WORDS)
      .setOutputCol(TF)
      .setVocabSize(NUM_TILE_WORDS)
    val titleTokenModel = tf.fit(titleTokenWords)
    val titleTokenTf = titleTokenModel.transform(titleTokenWords)
    titleTokenTf.cache()

    val idf = new IDF()
      .setInputCol(TF)
      .setOutputCol(TF_IDF)

    val dcGallIdTitleTokenWordsMatrix = idf.fit(titleTokenTf).transform(titleTokenTf).select(GALL_ID, TF_IDF)

    val titleWordsIds = titleTokenModel.vocabulary
    val dcGallIds = titleTokenTf.rdd.map(_.getString(0))
      .zipWithUniqueId()
      .map(_.swap)
      .collect()
      .toMap

    val vecRdd = dcGallIdTitleTokenWordsMatrix.select(TF_IDF).rdd.map { row =>
      Vectors.fromML(row.getAs[MLVector](TF_IDF))
    }
    vecRdd.cache()

    val mat = new RowMatrix(vecRdd)
    val svd = mat.computeSVD(NUM_K, computeU = true)

    val topWords = topWordsInTopConcepts(
      svd,
      10,
      5,
      titleWordsIds
    )
    val topGall = topGallInTopConcept(
      svd,
      10,
      1,
      dcGallIds
    )
    makeOutputFile(topWords, topGall)
  }

  def getDcGallInputDf(): DataFrame = {
    val inputSchema = new StructType()
      .add(GALL_ID, StringType)
      .add(REDUCE_TITLE, StringType)
    val boardIds = boardIdsData.collect()
    val rows = new util.ArrayList[Row]()
    boardIds.foreach { row =>
      val boardId = row.mkString
      val contentsRow = getDataFrame(INPUT_DATA_PATH + TARGET_DATE + boardId + FILE_TYPE)
        .select("title")
        .collect()
        .map(row => row.mkString)
        .mkString(" ")

      rows.add(Row(boardId, contentsRow))
    }
    spark.createDataFrame(rows, inputSchema)
  }

  def topWordsInTopConcepts(
                             svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int,
                             numTitleTerms: Int,
                             titleIds: Array[String]
                           ): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termsWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termsWeights.sortBy(-_._1)
      topTerms += sorted.take(numTitleTerms).map {
        case (score, id) => (titleIds(id), score)
      }
    }
    topTerms
  }

  def topGallInTopConcept(
                           svd: SingularValueDecomposition[RowMatrix, Matrix],
                           numConcepts: Int,
                           numGalls: Int,
                           docIds: Map[Long, String]
                         ): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topGalls = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeight = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topGalls += docWeight.top(numGalls).map {
        case (score, id) => (docIds(id), score)
      }
    }
    topGalls
  }

  def makeOutputFile(topWords: Seq[Seq[(String, Double)]], topGall: Seq[Seq[(String, Double)]]): Unit = {
    val outRows = new util.ArrayList[Row]()
    val outSchema = new StructType()
      .add(BEST_WORDS, StringType)
      .add(GALL_ID, StringType)
    for ((terms, docs) <- topWords.zip(topGall)) {
      outRows.add(Row(terms.map(_._1).mkString(", "), docs.map(_._1).mkString(", ")))
    }
    val outDf = spark.createDataFrame(outRows, outSchema)

    createCsvFileFromDataframe(outDf, OUTPUT_FILE_NAME)
  }

  def createCsvFileFromDataframe(dataFrame: DataFrame, dirName: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(OUTPUT_DATA_PATH + dirName)
  }

  def getDataFrame(path: String): DataFrame = {
    spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
  }
}