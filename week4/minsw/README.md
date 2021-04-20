<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 4


## What I've Learned 🙂
[ Part 2 구조적 API: DataFrame, SQL, Part 2Dataset ]

- CHAPTER 10 스파크 SQL
  - [블로그](https://minsw.github.io/2021/02/23/Spark-The-Definitive-Guide-10%EC%9E%A5/)
- CHAPTER 11 Dataset
  - [블로그](https://minsw.github.io/2021/03/01/Spark-The-Definitive-Guide-11%EC%9E%A5/)


## Online (Zoom)
> 2021.03.03

<img width="500" alt="week4" src="https://user-images.githubusercontent.com/26691216/110056496-4920ea80-7da2-11eb-93ef-1861117bc8cb.png">


- 예제가 적었고 + 앞의 내용을 안다는 가정을 하고 (SQL알지? DataFrame알지?) 설명해서 힘들었던 챕터들
- jin5335
  - 실무 _ hive -> spark로 전환준비중
    - hive는 metadata로 관리해서 건들여야하는 파티션만 건들이는데, spark는 다 건들이는 점 때문에 포기했었는데
    - 근데 스파크에서도 hive의 metadata 정보를 조회할수있는 것 같아서... => 재시도
  - 질문 : https://github.com/strange-study/ss-spark/issues/15
- minsw
  - database drop ?
    - hive는 테이블을 다 지워야 database drop이 가능했던듯
    - spark는? => [참고](https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-ddl-drop-database.html)
    - parameter 설정 가능 : `RESTRICT` (테이블이 남아있으면 지워지지않음, default) / `CASCADE` (테이블을 포함해서 모두 삭제)
  - 비상호 스칼라 쿼리(uncorrelated scalar query) ?
    - 언어의 scala가 아니라 상수 scala 의 의미인듯
  - SQL 전체 질문이긴한데
    - '서브쿼리 vs 뷰/테이블 따로 만들어서 쿼리' 의 퍼포먼스 차이 ?
- myeongki
  - 10장 테이블, 뷰, 데이터베이스의 차이와 주의점 정도 / 11장 데이터셋 사용의 장점





