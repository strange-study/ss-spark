# Chapter 12. RDD

## Todo

- [ ] RDD를 사용한 구현
- [ ] mapPartitons, foreachPartitions 사용

## 12.1 저수준 API란?

- 스파크 저수준 API
  - RDD, SparkContext, 분산형 공유 변수(accumulator, broadcast variable) 등이 있다.
- 저수준 API 사용 목적
  - 고수준 API에서 제공하지 않는 기능이 필요한 경우 ( 클러스터의 물리적 데이터 배치 etc)
  - 기존 코드 유지/보수
  - 사용자가 정의한 공유 변수를 다뤄야하는 경우.
- 스파크의 모든 워크로드는 저수준API로 변환되므로(스파크가 변환시킴), \
  **사용자는 고수준API**를 사용할 것을 권장한다.

## 12.2 RDD 개요

- DataFrame, Dataset -> RDD (complie)
- RDD: 불변성을 가지며 병렬로 처리할 수 있는 파티셔닝된 레코드의 모음
  - RDD 레코드: 자바, 스칼라, 파이썬의 객체

### RDD 종류

- Generic RDD
- Key-value RDD

### RDD 요소

- 파티션 목록
- 각 조각을 연산하는 함수
- 다른 RDD와의 의존성 목록
- Key-Value RDD를 위한 Partitioner
- 각 조각을 연산하기 위한 기본 위치 목록

- **RDD에는 사용자 정의 함수가 적용되므로, 파이썬에서 RDD를 사용하는 것은 상당한 성능정하를 일으킨다.**

## 12.3 RDD 생성하기

### 12.3.1 DataFrame과 Dataset으로 RDD 생성하기

```
# in python
spark.range(10).rdd
```

- `.rdd`: dataframe 객체의 rdd 메서드를 호출하면 Row 타입의 RDD를 얻을 수 있다.
- RDD의 데이터를 처리하기 위해서는 \
  Row 객체에서 데이터를 추출 or Row 객체를 올바른 데이터 타입으로 변환.
  - Row type: 스파크가 구조적 API에서 데이터를 표현하는 데 사용하는 내부 카탈리스트 포맷.
- `toDF()`: RDD에서 DataFrame으로 변환.

### 12.3.2 Local Collection으로 RDD 생성하기

```
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple" \
  .split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)

print(type(myCollection)) # list
print(type(words)) # pyspark.rdd.Rdd

words.setName("myWords")
words.name() # "myWords"
```

- parallelize(localCollection, # of partition): 단일 노드에 있는 컬렉션을 병렬 컬렉션으로 전환.
- setName(): rdd에 이름 지정하는 method.

### 12.3.1 데이터소스로 RDD 생성하기

```
spark.SparkContext.textFile("/some/path")
spakr.SparkContext.wholTextFiles("/some/path")
```

- DataSource API를 사용하는 것이 바람직(데이터소스 혹은 텍스트 파일을 이용해 직접 생성도 가능)
- RDD에는 DataFrame이 제공하는 **DataSource API**가 없음 \
  RDD는 RDD 간의 의존성 구조와 파티션 목록을 정의
- `SparkContext.textFile()`: 여러 텍스트 파일의 각 줄을 레코드로 가진 RDD 생성
- `SparkContext.wholeTextFiles()`: 텍스트 파일 하나를 레코드로 가진 RDD 생성

## 12.4 RDD 다루기

- DataFrame과 방식이 유사.
- RDD는 스파크 데이터 타입 대신 **자바나 스칼라의 객체**를 다룬다.
- DataFrame보다 helper method or function이 부족하다.

## 12.5 트랜스포메이션

- 트랜스포메이션을 통해 새로운 RDD 생성
- RDD에 포함된 데이터를 다루는 함수에 따라, 다른 RDD에 대한 의존성도 함께 정의.

### 12.5.\* distinct / filter / map / flatMap / sortBy / randomSplit

```
words.distinct().count()

def startsWithS(individual):
  return individual.startswith("S")

words.filter(lambda word: startsWithS(word)).collect() # ['Spark', 'Simple']

words2 = words.map(lambda word: (word, word[0], word.startswith("S")))
words2.filter(lambda record: record[2]).take(5)

words.flatMap(lambda word: list(word)).take(10)

words.sortBy(lambda word: len(word) * -1).take(10)

fiftyFiftySplit = words.randomSplit([0.9, 0.1])
print(fiftyFiftySplit[1].collect())
```

- filter: 불리언을 반환하는 함수를 입력으로 받고, 레코드 별로 적용하여 참이 레코드만을 반환한다.
- map: 주어진 입력을 원하는 값으로 반환하는 함수를 입력으로 받고, 레코드 별로 적용
- flatMap: map의 확장 버전으로, 단일 로우를 여러 로우로 변환해야할 때 사용.
- sort: 함수를 지정해, RDD 객체로부터 데이터를 추출한 다음 값을 기준으로 정렬한다.
- randomSplit: RDD를 임의로 분할해 RDD 배열을 만들 때 사용. 가중치와 난수 시드 지정

## 12.6 액션

- 액션: 데이터를 드라이버로 모으거나 외부 데이터소스로 내보낼 수 있다.

### 12.6.\* reduce / count / countApproxDistinct / countByValue / countByValueApprox / first / max & min / take

```
spark.sparkContext.parallelize(range(1, 21)).reduce(lambda x, y: x + y)

def wordLengthReducer(leftWord, rightWord):
  if len(leftWord) > len(rightWord):
    return leftWord
  else:
    return rightWord

words.reduce(wordLengthReducer)

words.count()
words.countApprox(400, 0.95) # 10
words.countApproxDistinct(0.05) # 5

words.countByValue()
words.countByValueApprox(10, 0.9) # 지원하지 않음

words.first()
words.max()
words.min()

words.take()
```

- reduce: RDD의 모든 값을 하나의 값으로 만들때 사용, 두 개의 입력을 받아 값 한개를 반환하는 함수를 인자로 받는다.
  - 파티션별로 연산이 되고 합쳐지기에 파티션에 대한 reduce 연산은 비결정적(원인이 동일하더라도 결과가 다를 수 있다)이다.
- count: RDD 전체 로우 수
- countApprox(timeout, confidence): count 함수의 근사치를 주어진 신뢰도를 가지고 제한시간 내에 계산한다.
- countApproxDistinct
  - streamlib 관련 논문(HyperLogLog 실전 활용: 최신 카디널리티 추정 알고리즘 엔지니어링)을 기반으로 한 두가지 구현체 존재.
  - 구현체에 대한 특징은 찾아보기...
- countByValue: RDD 값의 개수
  - key-value pair로 dict로 반환.
  - 결과 데이터셋을 드라이버의 메모리로 읽어들여서 처리하므로, 연산 결과가 적은 경우에만 사용.
- countByValueApprox(timeout, confidence): countByValue 함수의 근사치를 주어진 신뢰도를 가지고 제한시간 내에 계산한다.
- first: 데이터 셋의 첫 번째 값을 반환.
- max/min: 최대값과 최소값을 반환.
- take: 가져올 값의 개수를 인자로 받으며, 하나의 파티션의 수를 조사한 후 읽어야 하는 파티션 수를 예측한다.
  - takeOrdered
  - top: 암시적인 순서에 따라 최상위에서 부터 반환
  - takeSample: 임의 표본 데이터 반환

## 12.7 파일 저장하기

- 파일 저장: 데이터 처리 결과를 일반 텍스트 파일로 쓰는 것을 의미
- RDD를 사용하면 일반적인 **외부 데이터소스**에 저장이 안되고, \
  전체 파티션을 순회하면서 **외부 데이터베이스**에 저장해야 한다.
- 스파크는 각 파티션의 데이터를 파일로 저장한다.

### 12.7.\* saveAsTextFile / 시퀀스 파일 / 하둡 파일

- saveAsTextFile: 경로 및 압축코덱 지정 가능.
  - 압축 코덱은 하둡에서 사용 가능한 코덱을 사용해야 한다.
- 시퀀스 파일
  - Binary key-value pair로 구성된 flat file.
  - Map - reduce의 입출력 포맷
  - `saveAsObjectFile({path})` 사용
- 하둡 파일
  - 다양한 하둡 파일 포맷 지원
  - 클래스, 출력 포맷, 하둡 설정 그리고 압축 방식 지정 가능

## 12.8 캐싱

```
{rdd}.cache()

{rdd}.getStorageLevel()
```

- RDD를 캐시하거나 저장(persist) 할 수 있다.(DataFrame, Dataset과 동일)
- 기본적으로 메모리에 있는 데이터만을 대상으로 한다.
- 저장소 수준은 `org.apache.spark.storage.StorageLevel`을 통해 지정 가능
  - 메모리, 디스크, 둘의 조합 혹은 오프힙(off-heap)
  - `{rdd}.getStorageLevel()`: 저장소 수준 조회

## 12.9 체크포인팅

```
spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
{rdd}.checkpoint()
```

- 체크포인팅(checkpointing): RDD를 디스크에 저장하는 방식.
  - 저장된 RDD를 참조할 때는 디스크에 저장된 중간 결과 파티션을 참조.
  - DataFrame API에서는 사용 불가.
  - 메모리가 아닌 디스크에 저장한다는 사실을 제외하면 캐싱과 유사.

## 12.10 RDD를 시스템 명령으로 전송하기

- `pipe`: 파이핑 요소로 생성된 RDD를 외부 프로세스로 전달 가능하다.
  - 외부 프로세스는 파티션마다 한 번씩 처리해 결과 RDD를 생성
  - 각각의 입력 파티션 -> 개행 문자로 분할되어 입력 데이터 생성 -> 프로세스의 표준 입력(stdin)로 전달.
  - 프로세스 표준 출력(stdout) -> 각 줄은 출력 파티션의 요소가 된다.
  - 사용자 정의 함수를 통해, 출력 방식 변경 가능
  - `words.pipe("wc -l").collect()`: 파티션당 줄 수 출력.

### 12.10.1 mapPartitions

- 개별 파티션(이터레이터로 표현)에 대한 map 연산을 수행할 수 있다.
  - 클러스터에서 물리적 단위로 개별 파티션을 처리한다. (row 단위 아님)
- 파티션 그룹의 전체를 단일 파티션으로 모으고, 임의의 함수를 적용할 수도 있다.
- 예시

```
words.mapPartitions(lambda part: [1]).sum()
```

- words는 partitions 2개로 구성되어있으므로, 예시의 결과는 2이다.

* `mapPartitionsWithIndex`: mapPartitions 기능을 파티션 인덱스와 함께 사용할 수 있다.

```
def indexedFunc(partitionIndex, withinPartIterator):
    return ["partition: {} => {}".format(partitionIndex, x) for x in withinPartIterator]

words.mapPartitionsWithIndex(indexedFunc).collect()
```

### 12.10.2 foreachPartition

- 파티션의 모든 데이터를 순회하는 메서드로 \
  mapPartitions와 달리 결과를 반환하지 않는다. (return이 없다.)
- 각 파티션의 데이터를 데이터베이스에 저장하는 것과 같이 개별 파티션에서 특정 작업을 수행하는데 좋다.

### 12.10.3 glom

```
spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect() # [['Hello'], ['World']]
```

- 데이터셋의 모든 파티션을 배열로 변환하는 함수
- 데이터를 드라이버로 모으고, 데이터가 존재하는 파티션의 배열이 필요한 경우에 사용한다. \
  (드라이버로 데이터를 모으는 만큼, 파티션의 수가 많거나 크면 비정상적으로 종료될 수 있다.)

## 12.11 정리

## Questions

- 다른 RDD에 관한 의존성이란?
- RDD에는 DataFrame이 제공하는 **DataSource API**가 없고, RDD는 RDD 간의 의존성 구조와 파티션 목록을 정의
