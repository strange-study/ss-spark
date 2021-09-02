package com.sw.test

import java.io.File

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Success, Try}



/**
 * @author 민세원 (minSW)
 */

object ProjectApp {

  val INPUT_PATH: String = "/home/data/input"
  val OUTPUT_PATH: String = "/home/data/output"

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

  def getInputSchema() : StructType = {
    return StructType(Seq(
      StructField("gall_id", StringType, nullable = false),
      StructField("terms", ArrayType(StringType), nullable = false)
    ))
  }

  def getOutputSchema() : StructType = {
    return StructType(Seq(
      StructField("gall_ids", ArrayType(StringType), nullable = false),
      StructField("best_terms", ArrayType(StringType), nullable = false)
    ))
  }

  def getOutputSchema2() : StructType = {
    return StructType(Seq(
      StructField("term", StringType, nullable = false),
      StructField("best_galls", ArrayType(StringType), nullable = false)
    ))
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Project Application").getOrCreate()

    var res = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getInputSchema())

    val NUM_TERMS = 3000
    val K = 1000 // K < N
    val outputDir = new File(INPUT_PATH)
    val fileList: List[File] = if (outputDir.exists && outputDir.isDirectory) outputDir.listFiles.filter(_.isFile).toList else List[File]()

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
        res = res.union(outputDF)
      }
      catch {
        case ex : Exception => println(ex) // Skip Error
      }

    }

    // 2. TF
    val vocabModel = new CountVectorizer()
      .setInputCol("terms")
      .setOutputCol("termsFreqs")
      .setVocabSize(NUM_TERMS)
      .fit(res)
    val docTermsFreqs = vocabModel.transform(res)
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

    val numConcepts = res.count().toInt
    val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, 20, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, 5, docIds)

    var rowList = ListBuffer[Row]()
    val termsList = ListBuffer[String]()

    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      val bestTerms = terms.filter(_._2 > 0.001).map(t => f"${t._1}(${t._2 * 100}%.2f)")
      val bestDocs = docs.filter(_._2 > 0).map(t => f"${t._1}(${t._2 * 100}%.2f)")
      if (bestTerms.size > 0 && bestDocs.size > 0) {
        rowList += Row(bestDocs, bestTerms)
        termsList ++= terms.filter(_._2 > 0.001).map(_._1)
      }
    }

    val output = spark.createDataFrame(spark.sparkContext.parallelize(rowList.toList), getOutputSchema())

    output
      .withColumn("gall_rank", concat(lit("["), concat_ws(",", col("gall_ids")), lit("]")))
      .withColumn("best_words_rank", concat(lit("["), concat_ws(",", col("best_terms")), lit("]")))
      .select(col("gall_rank"), col("best_words_rank"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .csv(OUTPUT_PATH)

    val engine = new LSAQueryEngine(svd, termIds, docIds)
    val rowList2 = termsList.map(term => Row(term, engine.getTopDocsStringForTerm(term))).toList
    val output2 = spark.createDataFrame(spark.sparkContext.parallelize(rowList2), getOutputSchema2())
    output2
      .withColumn("best_galls_rank", concat(lit("["), concat_ws(",", col("best_galls")), lit("]")))
      .select(col("term"), col("best_galls_rank"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .csv(OUTPUT_PATH + "2")

    docTermsFreqs.unpersist()
    vecRdd.unpersist()
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
    return idWeights.filter(_._1 > 0).map(t => f"${docIds(t._2)}(${t._1}%.2f)")
  }
}