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

  val INPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/output/20210816"
  val OUTPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/result_10000/20210816"
  val TOP40_TERMS_BY_GALS = "top40_terms_by_gal"
  val TOP10_CONCEPTS = "top10_concepts"

  val tokenizer: RegexTokenizer = new RegexTokenizer().setInputCol("titles").setOutputCol("terms").setPattern("[ ]")
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
    df.orderBy("docId").coalesce(1).write.mode(SaveMode.Overwrite)
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
      StructField("gallery", StringType, false),
      StructField("date", StringType, false),
      StructField("title", StringType, true)
    ))
    var totalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], rawSchema)
    for (filePath <- filePaths) {
      System.out.println(filePath)

      val fileName: String = filePath.split("/").last.replace(".csv", "")
      val df = spark.read
        .options(Map("header" -> "true", "inferSchema" -> "true"))
        .csv(filePath)
        .withColumn("gallery", lit(fileName))
        .select("gallery","date", "title")

      totalDF = totalDF.unionByName(df)
    }

    val galTitlesDF = totalDF.groupBy("gallery", "date")
      .agg(collect_list("title").alias("title_list"))
      .withColumn("docId", concat_ws(",", $"gallery", split($"date", " ").getItem(0)))
      .withColumn("titles", concat_ws(" ", $"title_list"))
      .select("docId", "titles")

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
      .select("docId","termIdToTFIDFs")

    var exploded = docTermTFIDFMap.select($"docId", explode($"termIdToTFIDFs"))
      .withColumnRenamed("key", "termId")
      .withColumnRenamed("value","freq")

    val getTermFromIds = udf{id:Int => vocaDict(id)}
    val window = Window.partitionBy("docId").orderBy($"freq".desc)
    exploded = exploded.withColumn("row_number", row_number.over(window))
      .filter(s"row_number <= " + n)
      .withColumn("term", getTermFromIds($"termId"))
      .select("docId", "term", "freq")
      .withColumn("termFreq", concat(lit("("), $"term", lit(","), $"freq", lit(")")))

    val resultDF = exploded.groupBy("docId")
      .agg(collect_list("termFreq").alias("termFreq_list"))
      .withColumn("termFreqs", concat_ws(",",$"termFreq_list"))
      .withColumn("gallery", split($"docId", ",").getItem(0))
      .withColumn("date", split($"docId", ",").getItem(1))
      .select("gallery", "date", "termFreqs")

    resultDF.show(10, truncate = false)
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
    val TOP40_TERMS_BY_GALS_PATH = output_path + "/" + TOP40_TERMS_BY_GALS
    val TOP10_CONCEPTS_PATH = output_path + "/" + TOP10_CONCEPTS

    val galTitlesDF = makeGalTitlesOverPeriodDF(input_path)
    //val termsDF = tokenizeDocsDF(galTitlesDF)
    val termsDF = tokenizer.transform(galTitlesDF)

    galTitlesDF.show()

    termsDF.cache()
    termsDF.show()

    // calculate tfidf and svd
    val numTerms = 20000
    val (galTermFreqs, galTermMetrix, termIds, galIds) = calculateTFIDF(termsDF, numTerms)
    val top40TermsByGalsDF = makeTopNTermsByDocsDF(40, galTermMetrix, termIds)
    saveDFtoCSV(top40TermsByGalsDF, TOP40_TERMS_BY_GALS_PATH)

    /*
    val svd = calculateSVD(galTermMetrix)
    val topConceptDF = makeTopTermsAndDocsInTopConcepts(svd, termIds, galIds, 10, 20 ,5)
    saveDFtoCSV(topConceptDF, TOP10_CONCEPTS_PATH)*/
  }

  def main(args: Array[String]): Unit = {
    startAnalyze(INPUT_PATH, OUTPUT_PATH)
  }
}
