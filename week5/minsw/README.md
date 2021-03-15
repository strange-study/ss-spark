<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 5


## What I've Learned 🙂
[ Part 3 저수준 API ]

- CHAPTER 12 RDD
  - [블로그](https://minsw.github.io/2021/03/07/Spark-The-Definitive-Guide-12%EC%9E%A5/)


## Offline
> 2021.03.11

<img width="500" alt="week5" src="https://user-images.githubusercontent.com/26691216/111158587-8889db00-85db-11eb-9dc4-1f142a219514.jpg">

- minsw
  - ~~`p. 328` "강제로 Row 타입으로 변환할 필요가 없기 때문" 이 무슨 의미 인지?~~
  - => (Dataset과 RDD 개념을 착각했음)
  - 결론적으로 RDD는 실제로 사용하기보다 고수준 API의 내부/로우 레벨의 이해의 의미인 듯하다 (대부분의 케이스 고수준 사용 권장)
- myeongki
  - Q. rdd에서는 스칼라, 파이썬에서 쓰는 객체로 반환 => 현업에서 스파크에 의존하지않은 잡을 돌릴때 그걸(객체를) 뽑아서 사용하는 경우가 있나?
    - A. (실무 경험으로는) 할 수도 있겠으나 굳이 사용할 것 같지는 않다.
  - 그럼 RDD는 극/한의 튜닝이 아니면 안쓸거같은데, 쓸일이 있나 ?
  	- 사실상 로우레벨, 실제로 사용하는 경우는 책에서 말했듯이 거의 없을 듯
- jin5335
  - 질문 : https://github.com/strange-study/ss-spark/issues/19
  - rdd가 다른 rdd에 대한 의존성?
    - => 결국에는 실행계획, 의존관계
  - rdd에는 datasource api가 없다 ?
    - rdd는 일부는 사용가능하지만, dataframe에서는 rdd보다 더 많은 기능을 사용할 수 있다는 의미? (rdd는 바퀴를 새로만들어야하는데...)
    - => "즉, dataframe의 datasource api 처럼 많은기능이 있는건 아니다." 는 느낌으로 쓴건지