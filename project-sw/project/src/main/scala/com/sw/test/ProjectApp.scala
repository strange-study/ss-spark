package com.sw.test

import java.io.File
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Success, Try}


/**
 * @author 민세원 (minSW)
 */

object ProjectApp {
  val INPUT_PATH: String = "/home/ubuntu/ss-spark/project/data"
  val OUTPUT_PATH: String = "/home/ubuntu/ss-spark/project/output/minsw"

  val NUM_TERMS = 8000
  val K = 1000 // K < N

  val tokenizer: RegexTokenizer = new RegexTokenizer().setInputCol("title").setOutputCol("tokens").setPattern("[ ]")
  // FIXME : LIGHT vs FULL
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  def getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    def test(sentence: String) = Try {
      komoran.analyze(sentence).getNouns.asScala
    }

    test(sentence) match {
      case Success(lines) => lines
      case _ => Array[String]()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Project Application").getOrCreate()
    val ymd = LocalDate.now.format(DateTimeFormatter.BASIC_ISO_DATE) // TODO: Timezone check
    val inputPath = String.format("%s/%s", INPUT_PATH, ymd)
    val outputPath = String.format("%s/%s", OUTPUT_PATH, ymd)

    val inputDF = readTokenizedDocs(spark, inputPath)

    // 2. TF
    val vocabModel = new CountVectorizer().setInputCol("terms").setOutputCol("termsFreqs").setVocabSize(NUM_TERMS).fit(inputDF)
    val docTermsFreqs = vocabModel.transform(inputDF)
    docTermsFreqs.cache()

    val termIds: Array[String] = vocabModel.vocabulary

    // 3. IDF
    val idfModel = new IDF().setInputCol("termsFreqs").setOutputCol("tfidfVec").fit(docTermsFreqs)
    val docTermMatrix = idfModel.transform(docTermsFreqs)

    val docIds: Map[Long, String] = docTermsFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap

    // 4. SVD (M=USVt)
    // 'spark.ml' Vector => 'spark.mllib' Vector (for SVD)
    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map{ row => Vectors.fromML(row.getAs[MLVector]("tfidfVec"))} // : RDD[org.apache.spark.mllib.linalg.Vector]
    vecRdd.cache()
    val mat = new RowMatrix(vecRdd)
    val svd = mat.computeSVD(K, computeU=true)

    val numConcepts = inputDF.count().toInt.min(40)
    val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, 15, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, 5, docIds)

    val output = new OutputWriter(spark, outputPath)
    output.init()

    // write output1 (svd)
    var rowList = ListBuffer[Row]()
    val termsList = ListBuffer[String]()

    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      val bestTerms = terms.filter(_._2 >= 0.01).map(t => f"(${t._1},${t._2 * 100}%.2f)")
      val bestDocs = docs.filter(_._2 >= 0.01).map(t => f"(${t._1},${t._2 * 100}%.2f)")
      if (bestTerms.size > 0 && bestDocs.size > 0) {
        rowList += Row(bestDocs, bestTerms)
        termsList ++= terms.filter(_._2 > 0.001).map(_._1)
      }
    }

    output.write(output.getOutputDF1(rowList.toList), 1)

    // write output2 (top docs for term)
    val engine = new LSAQueryEngine(svd, termIds, docIds)
    val rowList2 = termsList.map(term => Row(term, engine.getTopDocsStringForTerm(term))).toList
    output.write(output.getOutputDF2(rowList2), 2)

    // write output3 (top 100 terms)
    output.write(getTop200Terms(docTermMatrix, termIds), 3)

    output.mergeOutput()
    // uncache
    docTermsFreqs.unpersist()
    vecRdd.unpersist()
  }

  def readTokenizedDocs(spark: SparkSession, inputPath: String) : DataFrame = {
    val inputSchema = StructType(Seq(
      StructField("gall_id", StringType, nullable = false),
      StructField("terms", ArrayType(StringType), nullable = false)
    ))

    var input = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
    val inputDir = new File(inputPath)

    val fileList: List[File] = if (inputDir.exists && inputDir.isDirectory) inputDir.listFiles.filter(_.isFile).toList else List[File]()

    for (file <- fileList) {
      try {
        val gallId = file.getPath.split("/").last.split(".csv")(0)

        val df = spark.read
          .options(Map("header" -> "true", "inferSchema" -> "true"))
          .csv(file.getPath)

        // 1. Tokenize
        val outputDF = tokenizer.transform(df)
          .withColumn("nouns", getNounsUdf(col("title")))
          .select(
            when(size(col("nouns")) > 0, col("nouns")).otherwise(col("tokens")).as("terms")
          )
          .select(explode(col("terms")))
          .agg(collect_list(col("col")).as("terms"))
          .select(
            lit(gallId).as("gall_id"),
            col("terms")
          )
        input = input.union(outputDF)
      }
      catch {
        case ex : Exception => println(ex) // Skip Error
      }
    }

    return input
  }

  // 의미 & 단어 관련성 (S*Vt) => V : local
  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int, numTerms: Int, termIds: Array[String]) : Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]
    val arr = v.toArray

    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1) // score desc sorting => (score, id)
      topTerms += sorted.take(numTerms).map {
        case (score, id) => (termIds(id), score) // => (term, score)
      }
    }
    topTerms
  }

  // 문서 & 의미 관련성 (U*S) => U : distributed
  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int, numDocs: Int, docIds: Map[Long, String]) : Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map { // top 1 (=> desc sort) => (score, uniqueKey)
        case (score, id) => (docIds(id), score) // (doc, score)
      }
    }
    topDocs
  }

  def getTop200Terms(docTermMatrix: DataFrame, termIds: Array[String]) : DataFrame = {
    val getTermIdVector = udf{ v: SparseVector => v.indices.zip(v.values).toMap }
    val getTermFromIds = udf{ id: Int => termIds(id) }

    val TF = docTermMatrix
      .withColumn("termIdVec", getTermIdVector(col("tfidfVec")))
      .select(col("gall_id"), explode(col("termIdVec")))
      .withColumnRenamed("key", "termId").withColumnRenamed("value","freq")
      .withColumn("term", getTermFromIds(col("termId")))

    return TF
      .orderBy(col("freq").desc)
      .groupBy(col("term"))
      .agg(
        round(sum(col("freq"))).as("freq"),
        concat_ws(",", collect_list(concat(col("gall_id"), lit("("), round(col("freq")), lit(")")))).as("galls")
      )
      .withColumn("rank", row_number().over(Window.orderBy(col("freq").desc)))
      .filter(col("rank") <= 200)
  }

}

class LSAQueryEngine(
                      val svd: SingularValueDecomposition[RowMatrix, Matrix],
                      val termIds: Array[String],
                      val docIds: Map[Long, String]) {

  // 특정 단어 -> 문서 유사도
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
  val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap

  def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until svd.V.numCols).map(i => svd.V(termId, i)).toArray
    val rowVec = Matrices.dense(rowArr.length, 1, rowArr)

    // Compute scores against every doc
    val docScores = US.multiply(rowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def getTopDocsStringForTerm(term: String): Seq[String] = {
    val idWeights = topDocsForTerm(idTerms(term))
    return idWeights.filter(_._1 > 0.1).map(t => f"(${docIds(t._2)},${t._1}%.2f)")
  }
}


class OutputWriter(val spark: SparkSession, val outputPath: String) {
  val TEMP_NAME = "temp"
  val RESULT_NAME = "result"

  def init(): Unit = {
    val dir = new File(outputPath)
    val tmpDir = new File(String.format("%s/%s", outputPath, TEMP_NAME))
    if (!dir.exists)
      dir.mkdir
    if (!tmpDir.exists)
      tmpDir.mkdir()
  }

  def getOutputDF1(rows: List[Row]): DataFrame = {
    val schema = StructType(Seq(
      StructField("gall_ids", ArrayType(StringType), nullable = false),
      StructField("best_terms", ArrayType(StringType), nullable = false)
    ))
    return spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  def getOutputDF2(rows: List[Row]): DataFrame = {
    val schema = StructType(Seq(
      StructField("term", StringType, nullable = false),
      StructField("best_galls", ArrayType(StringType), nullable = false)
    ))
    return spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  def write(df: DataFrame, outputNum: Int): Unit = {

    outputNum match {
      case 1 => writeOutput1(df)
      case 2 => writeOutput2(df)
      case 3 => writeOutput3(df)
    }
  }

  def mergeOutput(): Unit = {
    val outputDir = new File(outputPath)
    val dirList: List[File] = if (outputDir.exists && outputDir.isDirectory) outputDir.listFiles.filter(_.isDirectory).toList else List[File]()
    for (file <- dirList.flatMap(_.listFiles).filter(_.isDirectory).flatMap(_.listFiles).filter(_.isFile).filter(_.getPath.endsWith(".csv"))) {
      val number = file.getPath.split(getTempOutputPath(0)).last.split("/")(0)
      FileUtils.copyFile(file, new File(getRealOutputPath(number)))
    }
    for (dir <- dirList) {
      FileUtils.deleteDirectory(dir)
    }
  }

  private def writeOutput1(df: DataFrame): Unit =  {
    df
      .withColumn("gall_rank", concat_ws(",", col("gall_ids")))
      .withColumn("best_words_rank", concat_ws(",", col("best_terms")))
      .select(col("gall_rank").as("galls"), col("best_words_rank").as("words"))
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(getTempOutputPath(1))
  }

  private def writeOutput2(df: DataFrame): Unit = {
    df
      .withColumn("best_galls_rank", concat(concat_ws(",", col("best_galls"))))
      .select(col("term").as("word"), col("best_galls_rank").as("galls"))
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(getTempOutputPath(2))
  }

  private def writeOutput3(df: DataFrame): Unit = {
    df
      .select(
        col("rank"),
        col("freq"),
        col("term"),
        col("galls").as("top_galls")
      )
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(getTempOutputPath(3))
  }

  private def getTempOutputPath(num: Integer): String = {
    return if (num > 0) String.format("%s/%s/%d", outputPath, TEMP_NAME, num) else String.format("%s/%s/", outputPath, TEMP_NAME)
  }
  private def getRealOutputPath(name: String): String = {
    return String.format("%s/%s%s.csv", outputPath, RESULT_NAME, name)
  }

}