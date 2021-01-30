<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 1


## What I've Learned 🙂
[ Part 1 빅데이터와 스파크 간단히 살펴보기 ]
- CHAPTER 3 스파크 기능 둘러보기
  - [블로그](https://minsw.github.io/2021/01/24/Spark-The-Definitive-Guide-3%EC%9E%A5/)

[ Part 2 구조적 API: DataFrame, SQL, Part 2Dataset ]
- CHAPTER 4 구조적 API 개요
  - [블로그](https://minsw.github.io/2021/01/26/Spark-The-Definitive-Guide-4%EC%9E%A5/)

- CHAPTER 5 구조적 API 기본 연산
  - [블로그](https://minsw.github.io/2021/01/26/Spark-The-Definitive-Guide-5%EC%9E%A5/)


## Online (Zoom)
> 2021.01.28


<img width="500" alt="week1" src="https://user-images.githubusercontent.com/26691216/106351798-ced3f500-6321-11eb-8b30-d2b4e766ef93.png">


### week1 느꼈던 점
- myeongki
  - terminal 에서 작업을 할줄알고..
    - java 8 - spark 2 환경설정때문에 힘들었다 (`JAVA_HOME`)
    - => 싹 갈아엎고 인텔리제이로 설정을했다
  - 책에서 나온거랑은 좀 달랐다 (config 설정, spark value가 나오려면 지정 해줘야 하는 것들 등)
  - spark 가 어떻게 작동하는지 (실행계획 변환) 등의 전반적인 이해가 어려웠다 (DataFrame, RDD ~)
  - DSL 이 좋았다 (요즘 kotlin 공부하면서..)
    - SQL <-> Scala, Python 똑같이 만드는 예제가 도움이 되었다.
    - scala DSL 의 장점~
  - 이해가 안됐던 점
    - 기존에 배치 잡등에 익숙하지 않아서
    - 3장) 모델링이나 배치잡 => 스트리밍 등의 내용이 어려웠다

- (질문) Java 버전설정이 왜 필요했나요
  - spark compiler 자체는 올라가는데 (안올라가지는 않음)
    - count() 등의 함수 사용시 에러 발생
    - spark 3버전 이후는 java 11 도 지원된다고하는데 ..

- minsw
  - 뭐 공유했는지 잘 기억안남..
  - 번역의 이슈인지, '메소드 vs 함수' 다른 건지?

- jin5335
  - '분산처리 어떻게 하는지' 가 재밌었다
  - 2장 예제 - 연산이 가진 성능을 가지고 분산처리를 이렇게 할 수 있구나
  - (하둡, 얀, 등의) 클러스터와 스파크의 관계
    - 스파크 안에도 driver + executor
    - 클러스터를 쓰면 클러스터 안의 매니저 가 있고 .. (뒷장의 좀 더 심화된 부분까지 읽어봄)

- (질문) DataFrame 은 무엇인가?
  - 말그대로 데이터 (테이블) 이다. (row, column 같은)
  - 데이터의 표현 방법이 다른 것이다. SQL 하려면 (tempView)가 필요하고, 코드를 쓰려면 (DataFrame)이 필요하고
  - DataFrame 은 불변
  - 함수형 언어처럼 만들려는게 아닌가 (pure function, DAG ..)
    - -> 여러개가 얽힌 아키텍쳐일수록 그래야하지않을까

- (질문) select() vs selectExpr()  차이?
  - 차이가 없어보였다? => sum(), count() 등의 집계 함수 사용 가능
- (질문) 인자가 컬럼명, 컬럼.col(), expr 등 을 섞어쓰면 에러가 발생해야하는데?
  - scala에서는 에러 발생, pySpark에서는 에러 x?

    <img width="600" alt="week1-1" src="https://user-images.githubusercontent.com/26691216/106352018-3cccec00-6323-11eb-9a0b-89d75ed6dc4c.png">

    <img width="600" alt="week1-2" src="https://user-images.githubusercontent.com/26691216/106352021-40f90980-6323-11eb-9c88-9c7aef8d3b6c.png">
