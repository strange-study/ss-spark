<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 6


## What I've Learned 🙂
[ Part 3 저수준 API ]

- CHAPTER 13 RDD 고급개념
  - [블로그](https://minsw.github.io/2021/03/15/Spark-The-Definitive-Guide-13%EC%9E%A5/)


## Online (WhaleON)
> 2021.03.22

<img width="300" alt="week67" src="https://user-images.githubusercontent.com/26691216/113145764-d05e6280-9269-11eb-8df5-431684c3363d.PNG">

> 오랜만의 완전체 🎉 (feat. 큐티뽀짝 웨일 🐳)

- myeongki
  - flatmap() vs map() 차이점
    - rx에서는 map은 안되고 flatmap은 **obserable 타입** 으로 뽑아낼 수 있다를 차이로 구분하는 것 처럼..
      - 예) 타입을 바꾸는 용도로만 => map
      - 스트림에 다른 로직을 끼려고하면 => flatmap
    - A. 여기서의 flatmap과 map은 체이닝 (스트림)의 의미로 쓰지는 않은 것 같다. 단순히 1:1 이냐 1:N 이냐가 아닐지
- minsw
  - sampleByKey (sampleByKeyExtract) 함수 동작이 잘 이해가 안됨
- jin5335
  - 전체적으로 해당 챕터부터 책을 잘 참고하지 않게됨. 이해가 안되게 써놔서...
  - 챕터가 뒤로 갈수록 불친절.. (동감123)
- 기타 사담 (about 메모리 누수)
  - Facebook, 릭카나리 (heap dump 사용, 참조, 원래 제거 된거)
    - 책 추천 ? => JVM Performance & Optimizing
    - (".. 근데 저는 별로였습니다. (릭카나리 쓰셔요)")
  - Python 의 OOM 찾기
  - 스파크 OOM 발생 케이스 예
    - "브로드캐스트 조인" 할때 워커노드에 있는 모든 파티션을 드라이버로 모으게 되면서 발생 (일반적으로 워커노드는 메모리가 많지않은데)