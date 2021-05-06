<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->

# Week 11 (Practice 2 [#39](https://github.com/strange-study/ss-spark/issues/39))

> Spark 2.4.7 + Scala 2.12.13 + Gradle Build
> on Docker (Local)

### 6.1 문서 단어 행렬

### 6.2 데이터 구하기

- dump 데이터 (로컬) 사용 시
  - https://en.wikipedia.org/wiki/Special:Export
  - Megafauna, Geometry 카테고리로 export 후 wikidump.xml 로 저장
- 전체 데이터 사용 시
  ```bash
  curl -s -L https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 | bzip2 -cd | /root/hadoop-2.7.7/bin/hadoop fs -put - wikidump.xml
  ```


### 6.3 파싱하여 데이터 준비하기

- set up
```bash
# maven 설치 (ubuntu)
sudo apt update && sudo apt install maven
mvn package

# 라이브러리 다운로드
git clone https://github.com/sryza/aas.git
cd aas/ch06-lsa

# spark-shell 에 적용
export DEP=/root/aas/ch06-lsa/target/ch06-lsa-2.0.0-jar-with-dependencies.jar
cd /root/spark-2.4.7-bin-hadoop2.7/bin
./spark-shell --jars $DEP
```

- xml => plain text file (use Cloud9)

```scala
import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

val path="file:///root/wikidump.xml"  // 로컬파일 사용시 file:// 붙일 것
@transient val conf = new Configuration()
conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
val kvs = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
val rawXmls = kvs.map(_._2.toString).toDS()
// res: org.apache.spark.sql.DataFrame = [summary: string, value: string]
// 
// +--------------------+
// |               value|
// +--------------------+
// |<page>
//     <title...|
// |<page>
//     <title...|
// +--------------------+


import edu.umd.cloud9.collection.wikipedia.language._
import edu.umd.cloud9.collection.wikipedia._

def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
  val hackedPageXml = pageXml.replaceFirst(
    "<text bytes=\"\\d+\" xml:space=\"preserve\">", // 책이랑 export 한 xml파일 형태가 달라서 수정...
    "<text xml:space=\"preserve\">"
  )
  val page = new EnglishWikipediaPage()
  WikipediaPage.readPage(page, hackedPageXml)
  if (page.isEmpty) None
  else Some((page.getTitle, page.getContent))
}

val docTexts = rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)
// res: org.apache.spark.sql.DataFrame = [summary: string, _1: string ... 1 more field]
// 
// +---------+--------------------+
// |       _1|                  _2|
// +---------+--------------------+
// |Megafauna|Megafauna
// 
// In...|
// | Geometry|Geometry
// 
// ...|
// +---------+--------------------+
```


### 6.4 표제어 추출
- 불용어 파일 stopwords.txt => https://goo.gl/rWhMbE (/ch06-lsa/src/main/resources/stopwords.txt)

```scala
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties
import org.apache.spark.sql.Dataset

def createNLPPipeline(): StanfordCoreNLP = {
  val props = new Properties()
  props.put("annotators", "tokenize, ssplit, pos, lemma")
  new StanfordCoreNLP(props)
}

def isOnlyLetters(str: String): Boolean = {
  str.forall(c => Character.isLetter(c))
}

def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
: Seq[String] = {
  val doc = new Annotation(text)
  pipeline.annotate(doc)

  val lemmas = new ArrayBuffer[String]()
  val sentences = doc.get(classOf[SentencesAnnotation])
  for (sentence <- sentences.asScala;
       token <- sentence.get(classOf[TokensAnnotation]).asScala) {
    val lemma = token.get(classOf[LemmaAnnotation])
    if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
      lemmas += lemma.toLowerCase
    }
  }
  lemmas
}


val stopWords = scala.io.Source.fromFile("/root/stopwords.txt").getLines.toSet
// stopWords: scala.collection.immutable.Set[String] = Set(down, it's, ourselves, that's, for, further, she'll, any, there's, this, haven't, in, ought, myself, have, your, off, once, i'll, are, is, his, why, too, why's, am, than, isn't, didn't, himself, but, you're, below, what, would, i'd, if, you'll, own, they'll, up, we're, they'd, so, our, do, all, him, had, nor, before, it, a, she's, as, hadn't, because, has, she, yours, or, above, yourself, herself, she'd, such, they, each, can't, don't, i, until, that, out, he's, cannot, to, we've, hers, you, did, let's, most, here, these, hasn't, was, there, when's, shan't, doing, at, through, been, over, i've, on, being, same, how, whom, my, after, who, itself, me, them, by, then, couldn't, he, should, few, wasn't, again, while, their, not, with, ...

val bStopWords = spark.sparkContext.broadcast(stopWords) // 메모리 절약을 위한 브로드캐스트


val terms: Dataset[(String, Seq[String])] = docTexts.mapPartitions { iter =>
  val pipeline = createNLPPipeline()
  iter.map { case (title, contents) => 
    (title, plainTextToLemmas(contents, bStopWords.value, pipeline)) 
  }
}
// res: org.apache.spark.sql.DataFrame = [summary: string, _1: string]
// 
// +---------+--------------------+
// |       _1|                  _2|
// +---------+--------------------+
// |Megafauna|[megafauna, terre...|
// | Geometry|[geometry, geomet...|
// +---------+--------------------+
```


### 6.5 단어빈도-역문서빈도(TF-IDF) 계산하기

```scala
val termsDF = terms.toDF("title", "terms")
val filtered = termsDF.where(size($"terms") > 1)
//
// +---------+--------------------+
// |    title|               terms|
// +---------+--------------------+
// |Megafauna|[megafauna, terre...|
// | Geometry|[geometry, geomet...|
// +---------+--------------------+


import org.apache.spark.ml.feature.CountVectorizer

val numTerms = 20000
val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFreqs").setVocabSize(numTerms)
val vocabModel = countVectorizer.fit(filtered)
val docTermFreqs = vocabModel.transform(filtered)

docTermFreqs.cache()
//
// +---------+--------------------+--------------------+
// |    title|               terms|           termFreqs|
// +---------+--------------------+--------------------+
// |Megafauna|[megafauna, terre...|(2186,[1,2,3,4,5,...|
// | Geometry|[geometry, geomet...|(2186,[0,2,3,4,5,...|
// +---------+--------------------+--------------------+


import org.apache.spark.ml.feature.IDF

val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
val idfModel = idf.fit(docTermFreqs)
val docTermMatrix = idfModel.transform(docTermFreqs).select("title", "tfidfVec")
// 
// +---------+--------------------+--------------------+--------------------+
// |    title|           terms (x)|       termFreqs (x)|            tfidfVec|
// +---------+--------------------+--------------------+--------------------+
// |Megafauna|[megafauna, terre...|(2186,[1,2,3,4,5,...|(2186,[1,2,3,4,5,...|
// | Geometry|[geometry, geomet...|(2186,[0,2,3,4,5,...|(2186,[0,2,3,4,5,...|
// +---------+--------------------+--------------------+--------------------+

val termsIds: Array[String] = vocabModel.vocabulary
// termsIds: Array[String] = Array(geometry, largest, use, size, include, study, order, time, year, space, reach, extinct, america, increase, extinction, area, body, large, mammal, theory, mass, length, can, megafauna, giant, larger, geometric, also, ago, algebraic, century, human, euclidean, group, whale, plane, surface, topology, image, species, file, problem, may, animal, south, complex, megafaunal, concept, maximum, terrestrial, the, north, one, weight, rate, example, object, find, many, make, point, bird, predator, consider, curve, describe, theorem, differential, among, two, marine, volume, fish, land, early, line, angle, dimension, however, last, much, list, million, relative, methane, change, extant, know, important, similar, number, analysis, work, often, modern, riemann, late, pe...

val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap
// docIds: scala.collection.immutable.Map[Long,String] = Map(0 -> Megafauna, 1 -> Geometry)

```

## Online (WhaleON)
> 2021.05.06

진행 후 추가