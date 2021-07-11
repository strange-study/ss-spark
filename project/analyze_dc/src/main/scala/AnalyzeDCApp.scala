import java.io.File

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.ml.feature.{CountVectorizer, IDF, RegexTokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._


object AnalyzeDCApp extends SparkSessionWrapper {
  // todo: change to general path
  val INPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/output/20210702"
  val RAW_OUTPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/result/20210702/raw"
  val MAT_OUTPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/result/20210702/docTermMatrix"
  val RESULT_OUTPUT_PATH: String = "/Users/jinju/Documents/study/ss-spark/project/result/20210702/result"

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

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val filePaths: List[String] = getFilePaths(INPUT_PATH)

    val rawSchema = StructType(Array(
      StructField("gallery", StringType, false),
      StructField("title", StringType, true)
    ))
    var total_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], rawSchema)
    for (i <- 0 to 99) {
      val filePath = filePaths(i)
      val fileName: String = filePath.split("/").last.replace(".csv", "")
      val df = spark.read
        .options(Map("header" -> "true", "inferSchema" -> "true"))
        .csv(filePath)
        .withColumn("gallery", lit(fileName))
        .select("gallery", "title")

      total_df = total_df.unionByName(df)
    }

    total_df = total_df.groupBy("gallery")
      .agg(collect_list("title").alias("title_list"))
      .withColumn("titles", concat_ws(" ", $"title_list"))
      .select("gallery", "titles")

    total_df.write.mode(saveMode = "overwrite").option("header", true).csv(RAW_OUTPUT_PATH)
    val concatTitles = spark.read.option("header", true).csv(RAW_OUTPUT_PATH)
    concatTitles.show(truncate=false)

    val termsDF = concatTitles
      .withColumn("terms", getMorphsUDF($"titles"))
      .where(size($"terms") > 0)

    termsDF.cache()
    termsDF.show()

    val numTerms = 5000
    val vocabModel = new CountVectorizer()
      .setInputCol("terms").setOutputCol("termFreqs")
      .setVocabSize(numTerms)
      .fit(termsDF)

    val docTermFreqs = vocabModel.transform(termsDF)
    docTermFreqs.cache()
    docTermFreqs.show()

    val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)

    var docTermMetrix = idfModel.transform(docTermFreqs)

    docTermMetrix.write.mode(saveMode = "overwrite").parquet(MAT_OUTPUT_PATH)
    docTermMetrix = spark.read.parquet(MAT_OUTPUT_PATH)

    val getTermTFIDFMap = udf{v:SparseVector => v.indices.zip(v.values).toMap}
    val docTermTFIDFMap = docTermMetrix.withColumn("termIdToTFIDFs", getTermTFIDFMap($"tfidfVec"))
        .select("gallery","termIdToTFIDFs")

    docTermTFIDFMap.cache()
    docTermTFIDFMap.show()

    var exploded = docTermTFIDFMap.select($"gallery", explode($"termIdToTFIDFs"))
        .withColumnRenamed("key", "termId")
        .withColumnRenamed("value","freq")

    val vocaDict = vocabModel.vocabulary
    val getTermFromIds = udf{id:Int => vocaDict(id)}
    val window = Window.partitionBy("gallery").orderBy($"freq".desc)
    exploded = exploded.withColumn("row_number", row_number.over(window))
        .filter("row_number < 21")
        .withColumn("term", getTermFromIds($"termId"))

    exploded.cache()
    exploded.show()

    val resultDF = exploded.groupBy("gallery")
        .agg(collect_list("term").alias("term_list"))
        .withColumn("terms", concat_ws(",",$"term_list"))
        .select($"gallery", $"terms")
    resultDF.show(truncate = false)
    resultDF.write.mode(SaveMode.Overwrite).option("header", "true").csv(RESULT_OUTPUT_PATH)
  }
}
