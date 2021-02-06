<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 2


## What I've Learned 🙂
[ Part 2 구조적 API: DataFrame, SQL, Part 2Dataset ]

- CHAPTER 6 다양한 데이터 타입 다루기
  - [블로그](https://minsw.github.io/2021/02/02/Spark-The-Definitive-Guide-6%EC%9E%A5/)
- CHAPTER 7 집계 연산
  - [블로그](https://minsw.github.io/2021/02/03/Spark-The-Definitive-Guide-7%EC%9E%A5/)


## Online (Zoom)
> 2021.02.04

<img width="500" alt="week3" src="https://user-images.githubusercontent.com/26691216/107124772-21e11580-68e9-11eb-9272-57600772ec8d.png">


- myeongki
  - p.217 group-by 예제 코드 
    - Q. python == scala == sql 같은 동작을 하는것 같지않은데?
    - A. 1단계)(python == scala) 2단계)sql 임. 
  - 인상깊었던 것은? => UDF의 강력함
    - python, scala 같이 쓸땐 스칼라가 낫다
    - null 있으면 미리 처리해주는게좋다
    - 포멧팅은 명시적으로 해주는게 좋을거같다
    - (자세한 내용은 https://github.com/strange-study/ss-spark/tree/main/week2/myeongki 참고)

- minsw 
  - p.149 로우정렬하기 예제 코드에 문법오류? (사실 week1 내용..)
    - `df.orderBy(expr("count desc")).show(2)`
  - 책도 사람이 만드는거라 어쩔수없이 완벽할수없다..

- (질문) window vs group-by
  - jin5335 : 경험에 따른 차이점
    - A, B, C 컬럼이 있고
    - group by (A, B) 를 하면서 count(C), sum(C)를 동시에 알고싶다면?
    - => `GROUP BY` (X) window 함수 사용 (O)
  - 그럼 항상 window가 더 맞는것인가?
    - 퍼포먼스 vs 기능적 요구사항에 따라?
  - 윈도우의 또 다른 장점은 windowSpec 도 여러 개로 만들어서 한번에 적용 가능 (책 예제 참고)

- (질문) 롤업 vs 큐브 + group_id 의 의미?
  - => 책의 group_id 를 좀 더 자세하게 봤을때로는..
    - minsw : 큐브는 롤업 개념을 포함하면서, 그룹화대상 컬럼으로 만들 수 있는 모든 조합의 경우의 수를 다 포함한다?
    - [7.4.3 grouping_id()](https://minsw.github.io/2021/02/03/Spark-The-Definitive-Guide-7%EC%9E%A5/#7-4-%EA%B7%B8%EB%A3%B9%ED%99%94-%EC%85%8B) 예제 참고
  - 정리하자면
    - 롤업은 grouping_id=0 의 정보 및 grouping_id=max 정보만 제공
    - 큐브는 grouping_id=0~max(아마도 `2^컬럼개수 - 1` 개) 정보 제공

