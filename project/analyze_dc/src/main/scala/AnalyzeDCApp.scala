import java.io.File
import java.util

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object AnalyzeDCApp extends SparkSessionWrapper {
  import spark.implicits._

  val INPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/output/20210702"
  val INPUT_PATH2: String = "/Users/jinju/Documents/study/ss-spark/project/output2/20210721"
  val OUTPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/result/20210702"
  val OUTPUT_PATH2: String = "/Users/jinju/Documents/study/ss-spark/project/result2/20210721"
  val TOP20_TERMS_BY_GALS = "top20_terms_by_gal"
  val TOP10_CONCEPTS = "top10_concepts"

  val tokenizer: RegexTokenizer = new RegexTokenizer().setInputCol("title").setOutputCol("tokens").setPattern("[ ]")
  val getMorphsUDF: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    val TAGS: List[String] = Array("NNG", "NNP", "VV", "VA", "MM", "IC").toList

    komoran.analyze(sentence).getMorphesByTags(TAGS.asJava).asScala
  }

  def getFilePaths(dir_path: String): List[String] = {
    val dir = new File(dir_path)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).map(file => file.getPath()).toList
    } else{
      List[String] ()
    }
  }

  def saveDFtoCSV(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .options(Map("header" -> "true", "compression" -> "none"))
      .csv(path)
  }

  def makeGalTitlesDF(path: String): DataFrame = {
    val filePaths: List[String] = getFilePaths(path)
    val rawSchema = StructType(Array(
      StructField("gallery", StringType, false),
      StructField("title", StringType, true)
    ))
    var totalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], rawSchema)
    for (i <- 0 to 99) {
      val filePath = filePaths(i)
      val fileName: String = filePath.split("/").last.replace(".csv", "")
      val df = spark.read
        .options(Map("header" -> "true", "inferSchema" -> "true"))
        .csv(filePath)
        .withColumn("gallery", lit(fileName))
        .select("gallery", "title")

      totalDF = totalDF.unionByName(df)
    }

    val galTitlesDF = totalDF.groupBy("gallery")
      .agg(collect_list("title").alias("title_list"))
      .withColumn("titles", concat_ws(" ", $"title_list"))
      .select("gallery", "titles")

    galTitlesDF
  }

  def makeGalTitlesOverPeriodDF(path: String): DataFrame = {
    val filePaths: List[String] = getFilePaths(path)
    val rawSchema = StructType(Array(
      StructField("date", StringType, false),
      StructField("title", StringType, true)
    ))
    var totalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], rawSchema)
    for (filePath <- filePaths) {
      val fileName: String = filePath.split("/").last.replace(".csv", "")
      val df = spark.read
        .options(Map("header" -> "true", "inferSchema" -> "true"))
        .csv(filePath)
        .select("date", "title")

      totalDF = totalDF.unionByName(df)
    }

    val galTitlesDF = totalDF.groupBy("date")
      .agg(collect_list("title").alias("title_list"))
      .withColumn("titles", concat_ws(" ", $"title_list"))
      .select("date", "titles")

    galTitlesDF
  }

  def tokenizeDocsDF(docsDF: DataFrame): DataFrame = {
    val termsDF = docsDF
      .withColumn("terms", getMorphsUDF($"titles"))
      .where(size($"terms") > 0)

    termsDF
  }

  def calculateTFIDF(termsDF: DataFrame, vocabSize: Int): (DataFrame, DataFrame, Array[String], Map[Long, String]) = {
    // count term freqs
    val countVectorizer = new CountVectorizer()
      .setInputCol("terms").setOutputCol("termFreqs")
      .setVocabSize(vocabSize)
    val vocabModel = countVectorizer.fit(termsDF)
    val docTermFreqs = vocabModel.transform(termsDF)

    // calculate tf-idf
    val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMetrix = idfModel.transform(docTermFreqs)
    val termIds = vocabModel.vocabulary
    val docIds = docTermFreqs.rdd.map(_.getString(0)).
      zipWithUniqueId().
      map(_.swap).
      collect().toMap

    (docTermFreqs, docTermMetrix, termIds, docIds)
  }

  def makeTopNTermsByDocsDF(n: Int, docTermMetrix: DataFrame, vocaDict: Array[String])= {
    val getTermTFIDFMap = udf{v:SparseVector => v.indices.zip(v.values).toMap}
    val docTermTFIDFMap = docTermMetrix.withColumn("termIdToTFIDFs", getTermTFIDFMap($"tfidfVec"))
      .select("gallery","termIdToTFIDFs")

    var exploded = docTermTFIDFMap.select($"gallery", explode($"termIdToTFIDFs"))
      .withColumnRenamed("key", "termId")
      .withColumnRenamed("value","freq")

    val getTermFromIds = udf{id:Int => vocaDict(id)}
    val window = Window.partitionBy("gallery").orderBy($"freq".desc)
    exploded = exploded.withColumn("row_number", row_number.over(window))
      .filter(s"row_number <= " + n)
      .withColumn("term", getTermFromIds($"termId"))

    val resultDF = exploded.groupBy("gallery")
      .agg(collect_list("term").alias("term_list"))
      .withColumn("terms", concat_ws(",",$"term_list"))
      .select($"gallery", $"terms")

    resultDF
  }

  def calculateSVD(docTermMetrix:DataFrame): SingularValueDecomposition[RowMatrix, Matrix] = {
    val vecRdd = docTermMetrix.select("tfidfVec").rdd.map { row =>
      Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }
    vecRdd.cache()

    val mat = new RowMatrix(vecRdd)
    val k = 1000
    val svd = mat.computeSVD(k, computeU=true)

    svd
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                            numConcepts: Int,
                            numTerms: Int,
                            termIds: Array[String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {
        case (score, id) => (termIds(id), score)
      }
    }
    topTerms
  }

  def topDocsInTopConcepts (
                             svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int, numDocs: Int, docIds: Map[Long, String])
  : Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map {
        case (score, id) => (docIds(id), score)
      }
    }
    topDocs
  }

  def makeTopTermsAndDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], termIds: Array[String], docIds: Map[Long, String],
                                 numConcepts: Int, numTerms: Int, numDocs: Int) = {
    val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, numTerms, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, numDocs, docIds)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }

    val resultRows = new util.ArrayList[Row]()
    val resultSchema = StructType(Array(
      StructField("topDocs", StringType, true),
      StructField("topTerms", StringType, true)
    ))
    for ((terms, docs) <- topConceptDocs.zip(topConceptTerms)) {
      resultRows.add(Row(docs.map(_._1).mkString(", "), terms.map(_._1).mkString(", ")))
    }
    val topConceptDF = spark.createDataFrame(resultRows, resultSchema)
    topConceptDF.show(truncate=false)

    topConceptDF
  }

  def startAnalyze(input_path: String, output_path: String) = {
    val TOP20_TERMS_BY_GALS_PATH = output_path + "/" + TOP20_TERMS_BY_GALS
    val TOP10_CONCEPTS_PATH = output_path + "/" + TOP10_CONCEPTS

    val galTitlesDF = makeGalTitlesDF(INPUT_PATH)
    val termsDF = tokenizeDocsDF(galTitlesDF)

    termsDF.cache()
    termsDF.show()

    // calculate tfidf and svd
    val numTerms = 5000
    val (galTermFreqs, galTermMetrix, termIds, galIds) = calculateTFIDF(termsDF, numTerms)
    val svd = calculateSVD(galTermMetrix)

    val top20TermsByGalsDF = makeTopNTermsByDocsDF(20, galTermMetrix, termIds)
    saveDFtoCSV(top20TermsByGalsDF, TOP20_TERMS_BY_GALS_PATH)

    val topConceptDF = makeTopTermsAndDocsInTopConcepts(svd, termIds, galIds, 10, 10 ,1)
    saveDFtoCSV(topConceptDF, TOP10_CONCEPTS_PATH)
  }

  def main(args: Array[String]): Unit = {
    startAnalyze(INPUT_PATH, OUTPUT_PATH)
    startAnalyze(INPUT_PATH2, OUTPUT_PATH2)
  }
}
