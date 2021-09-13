
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util
import scala.collection.mutable
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
  val DATE = "DATE"
  val SCORE_WORDS = "SCORE_WORDS"
  val TARGET_BOARD_ID = "bitcoins"

  val spark: SparkSession = SparkSession.builder.appName("sparkTest")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "192.168.1.4")
    .getOrCreate()

  val boardIdsData: DataFrame = getDataFrame(INPUT_DATA_PATH + BOARD_IDS)

  val NUM_TILE_WORDS = 96214

  val NUM_K = 800

  val NUM_VIEW = 200

  val OUTPUT_FILE_NAME = "result-" + s"view-$NUM_VIEW" + s"k-$NUM_K"

  val TOKEN_PATTERN = "[ ]"

  val wordScoreMap: mutable.HashMap[String, mutable.HashMap[String, Double]] = mutable.HashMap()

  def main(args: Array[String]) {
    executeSemanticAnalysis()
    getDataFrame(OUTPUT_DATA_PATH + OUTPUT_FILE_NAME).show()
  }


  def executeSemanticAnalysis(): Unit = {

    val inputDf = getDcGallInputDf()

    val tokenizer = new RegexTokenizer()
      .setInputCol(REDUCE_TITLE)
      .setOutputCol(TITLE_TOKEN_WORDS)
      .setPattern(TOKEN_PATTERN)

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
      15,
      titleWordsIds
    )
    val topGall = topGallInTopConcept(
      svd,
      10,
      5,
      dcGallIds
    )
    makeOutputFile(topWords, topGall)
  }

  def getDcGallInputDf(): DataFrame = {
    val inputSchema = new StructType()
      .add(GALL_ID, StringType)
      .add(REDUCE_TITLE, StringType)
    val rows = new util.ArrayList[Row]()
    val dateRow = getDataFrame(INPUT_DATA_PATH + TARGET_BOARD_ID + FILE_TYPE)
      .select("date")
      .collect()
      .map(row => row.mkString.substring(0, 10))
      .distinct
    dateRow.foreach(date => {
      val df = getDataFrame(INPUT_DATA_PATH + TARGET_BOARD_ID + FILE_TYPE)
      val contentsRow = df
        .select("title")
        .where(df.col("date").contains(date) and (df.col("view") > NUM_VIEW))
        .collect()
        .map(row => row.mkString)
        .mkString(" ")

      rows.add(Row(TARGET_BOARD_ID + " " + date, contentsRow))
    }
    )
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
    calculateWordScore(topWords, topGall)
    val outRows = new util.ArrayList[Row]()
    val outSchema = new StructType()
      .add(DATE, StringType)
      .add(SCORE_WORDS, StringType)
    wordScoreMap.foreach(dateAndScoreWords => {
      outRows.add(Row(dateAndScoreWords._1, dateAndScoreWords._2.mkString(", ")))
    })
    val outDf = spark.createDataFrame(outRows, outSchema)
    createCsvFileFromDataframe(outDf, OUTPUT_FILE_NAME)
  }

  def calculateWordScore(topWords: Seq[Seq[(String, Double)]], topGall: Seq[Seq[(String, Double)]]): Unit = {
    var conceptWeight = topWords.zip(topGall).size + 1
    for ((terms, docs) <- topWords.zip(topGall)) {
      docs.foreach(docAndScore => {
        val doc = docAndScore._1
        val docScore = if (docAndScore._2 < 0) {
          0
        } else {
          docAndScore._2 * 100
        }
        terms.foreach(termAndScore => {
          val term = termAndScore._1
          val termScore = if (termAndScore._2 < 0) {
            0
          } else {
            termAndScore._2 * 100
          }
          wordScoreMap.getOrElseUpdate(doc, mutable.HashMap())

          val resultScore = math.pow(termScore * docScore, 2) * conceptWeight // (단어 점수 * 문서 점수)의 스코어가 비슷한 경우에 좀 더 확실한 차이를 주기 위하여 2제곱 사용.
          wordScoreMap(doc)(term) = wordScoreMap(doc).getOrElseUpdate(term, 0) + resultScore
        })
      })
      conceptWeight -= 1
    }
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