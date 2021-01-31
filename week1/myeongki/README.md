# 1주차 정리

## 1장

- 환경 설정 -> 처음에는 터미널에서 작업하려 했지만.. 인텔리제이로 환경 설정 변경 (자바 버젼으로 꽤 큰 삽질.. jEnv 꿀팁..으로 해결)

### SBT 설정

```
name := "sparkTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
```

### spark 변수 선언
```
val spark = SparkSession.builder.appName("sparkTest").config("spark.master", "local").getOrCreate()
```
- 터미널에서는 따로 변수 선언 없이 진행했는데.. 흠..

## 2장

- 간단하게 스파크가 어떤식으로 동작하는지 파악
- 스파크에서 실행 계획을 확인할 수 있고 이때 sql이나 data frame이 동일한 실행 계획을 갖는 다는 점이 흥미로움.
- 예제를 통하여 data frame dsl이 강점을 느낄 수 있었음.

## 3장

- 스파크에서 사용할 수 있는 기능들을 둘러봄... 무슨 소리인지..

## 4장

- 데이타프레임 vs 데이타 셋 ??? : dataframe은 런타임 시에 타입 결정, dataset은 컴파일 시에 결정 -> 결론은 dataframe은 스파크에 최적화된 내부 포맷을 사용할 수 있음.. 하지만 컴파일 시간에 엄격한 타입 검증이 필요한 경우에는 dataset을 쓰자.

- 스파크 언어 표? : 결국 해당 표에서 맵핑되는 타입들을 카탈리스크 엔진을 통하여 다시 스파크로 동일하게 표현하기 때문에 어떤 언어로 로직을 구성하여도 수행 시간은 비슷하다는 이야기 같다. -> 스파크를 하나의 언어로 보아도 된다고 한다..

## 5장

```

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

object TestApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("sparkTest")
      .config("spark.master", "local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val manualSchema = new StructType(Array(
      StructField("some", StringType, nullable = true),
      StructField("col", StringType, nullable = true),
      StructField("names", LongType, nullable = false)))

    val rows = Seq(Row("Hello", null, 1L))
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, manualSchema)
    df.selectExpr("col as ll", "col").show()
    df.show()

    spark.stop()
  }
}


```

- "Utils: Service 'sparkDriver' could not bind on a random free port " : 로컬 ip 속성 추가로 해결

## 6장?

