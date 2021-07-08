package com.sw.test

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.SparseVector
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran

import scala.collection.JavaConverters._
import java.io.File

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vectors, Vector => MLLibVector}
import org.apache.spark.ml.linalg.{Vector => MLVector}

import scala.util.{Success, Try}


/**
 * @author 민세원 (min.saewon@navercorp.com)
 */

object ProjectApp {

  val INPUT_PATH: String = "/home/data/input"
  val OUTPUT_PATH: String = "/home/data/output"
  val NUM_TERMS = 20

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

  def getSchema() : StructType = {
    return StructType(Seq(
      StructField("gall_id", StringType, nullable = false),
      StructField("best_words", ArrayType(StringType), nullable = false)
    ))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Project Application").getOrCreate()

    import spark.implicits._

    var res = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema())

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
            col("title"),
            when(size(col("nouns")) > 0, col("nouns")).otherwise(col("tokens")).as("terms")
          )
          .filter(size(col("terms")) > 0)

        // 2. TF
        val model = new CountVectorizer()
          .setInputCol("terms")
          .setOutputCol("termsFreqs")
          .setVocabSize(NUM_TERMS)
          .fit(outputDF)
        val termIds: Array[String] = model.vocabulary

        val docTermsFreqs = model.transform(outputDF)
        docTermsFreqs.cache()

        // 3. IDF
        val idfModel = new IDF().setInputCol("termsFreqs").setOutputCol("tfidfVec").fit(docTermsFreqs)
        val docTermMatrix = idfModel.transform(docTermsFreqs)

        val docRdd = docTermMatrix.select("tfidfVec")

        val words = docRdd.flatMap { ele =>
          var idx = -1
          val ids = ele.getAs[SparseVector](0).toDense.toArray.map(value => {
            idx = idx + 1
            (idx, value)
          })
          ids.filter(v => { v._2 == 0.0 }).map { v => termIds(v._1) }
        }

        val bestWords = words.distinct().select(
          lit(gallId).as("gall_id"),
          collect_list("value").as("best_words")
        )

        // Append Result
        res = res.union(bestWords)
        docTermsFreqs.unpersist()
      }
      catch {
        case ex : Exception => println(ex) // Skip Error
      }

    }

    res.withColumn("res", concat(lit("["), concat_ws(",", col("best_words")), lit("]")))
      .select(col("gall_id"), col("res"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .csv(OUTPUT_PATH)

  }
}


//    // row ID - doc TITLE
//    //val docIds = docTermsFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap
//
//    // SVD
//    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map{ row => Vectors.fromML(row.getAs[MLVector]("tfidfVec"))}
//    vecRdd.cache()
//    val mat = new RowMatrix(vecRdd)
//    val k = 1000
//    val svd = mat.computeSVD(k, computeU=true)