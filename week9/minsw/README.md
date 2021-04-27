<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->

# Week 9 (Practice 1 [#32](https://github.com/strange-study/ss-spark/issues/32))

> Spark 2.4.7 + Scala 2.12.13 + Gradle Build
> on Docker (Local)

```bash
# Submitting Application (spark-submit)
./spark-2.4.7-bin-hadoop2.7/bin/spark-submit --class "com.sw.test.PracticeApp" --master local ss-spark-1.0-SNAPSHOT.jar
```

- Spark Application 코드 (PracticeApp) : https://github.com/minSW/ss-spark-practice
- Output : [/output](./output)

## Online (WhaleON)
> 2021.04.22

<img width="500" alt="week9" src="https://user-images.githubusercontent.com/26691216/116266600-988a0280-a7b6-11eb-82ef-707b50d793af.png">

- myeongki
  - multi thread 인지는 모르겠다
    - 그런부분까지 검토하면 좋았을텐데
    - 일단은 결과를 뽑는거에만 집중을했어요
  - https://github.com/strange-study/ss-spark/blob/main/week9/cho/sparkTest/src/main/scala/TestApp.scala
  - scala 문법을 몰라서 아쉬웠던 점
    - kotlin 이라면 내가 써놓은 변수중..  (const value로 선언 해서 쓴다든지)
    - scala 언어의 **철학** 을 모른채로 알고리즘 풀듯 풀어서 아쉽다
    - 스칼라 책을 보고 했다면 좀 더 예쁘게 짰을텐데..
- 질문
  - Q1. 자동으로 spark에서는 잡을 던지나요? 직렬처리 vs 병렬처리?
    - csv -> 컬럼추출 -> for ()
  - A1. (jin5335) 직렬 처리됩니다
    - for (본인이쓴거..) => 직렬 (해당 잡이 끝나야 다음 것 실행)
    - 즉, 병렬처리는되는데 for문이아닌 수평분할 (1~1000, 1001~2000, ...)
  - Q2. 그럼 다른쓰레드에서 돌리고싶다면? (for문 대신에.. 각 개별 병렬처리를 하고싶다면)
    - 이전 답변에서 for문이 한 job이 끝날때까지 기다린다면
    - 새로운 쓰레드를 만들어서 할수 있는지, (=> 코드내에서 쓰레드를 돌려야 할 것)
    - 이런식의 구동이 스파크차원에서 지원을하는지?
  - A2. "스파크의 철학과 같지않다."
    - 스파크의 '병렬'은 같은 연산을 쪼개서 input 만 바꿔서하는건데, 그게 스파크 관점의 병렬은 아니다.
    - => **"한번 찾아보자......!"** (비동기처리를 지원하는 게 있을지?)
- 실무에서는 어떻게?
  - Q. Python (or Scala) 성능측정어떻게해요? (ex. dump? ..)
  - => 안합니다. 돌리고 실행시간으로 비교
- 즉, 성능은 "실행시간 축소"를 기준으로 잡았다
  - 성능테스트라는건...
    - jin5335) 아마도.. 가능한건 datasize에 대한 테스트
    - myeongki) 성능이라는게 보는 관점에 따라 달라질 수 있다
      - 실행시간이 같다고해서 성능적으로 같은 결과는 아니다.
      - 리소스(메모리, cpu) 의 관리는? (profiler같은..)
    - minSW) 데이터 처리는 최적화가 '리소스'가 대상은 아닌거같다. '소요 시간'에 집중하는 느낌...
      - 우리가 하는 최적화는 _ 메모리 누수 잡기? API 콜수 잡기? 이런..
  - 사실..
    - 엔지니어링 데이를 봤어요 (2021 Engineering day 참고)
    - spark 로 e-tech에서 쓰는거 봤는데, 특정 프로파일러를써서.. 메모리와 cpu 최적화... 
- 최신 닥면 이슈
  - 문제상황
    - 한시간에 배치를 X분짜리 X개를 돌리는데 X일치를 돌리려고보니 2일이 걸림
    - 한꺼번에 돌리면 시간단축이 되지 않을까? 했다가 망함
  - 왜?
    - spark = lazy evaluation
    - for문써봤자... 성능나아지는거없음...캐싱해야하고....
  - 결론은
    - 무식하게만 안돌리면된다
    - spark할때 for문쓰지마라.. 캐시명령어 따로써줘라.. 교훈...
- 스파크의 디버그
  - 스파크는 디버그도 어렵다..........
  - 데이터양에 따라 에러의 유무도 다를때도 있음
  - Q. Spark Application 서브밋할때 알수없는 로그들 왕창.. 디버그는 어떻게?
    - print찍고 spark log level 변경 or 로깅 설정을 따로
    - => 이럴거면 spark shell로 한번 돌리는게..
      - 이것이 최선인가요..?
- Q. 왜 write시 이런저런 이름이 나오는지?
  - a.csv/어쩌구저쩌구.csv, 어쩌구저쩌구.csv.crc, _SUCCESS
  - => 해당 Output은 그렇게 나올수밖에.. 다만 읽을땐 **디렉토리 네임만 설정해도 읽힘** (=> csv)

### 그 외 후기

- window vs 그냥 sql? 뭐가달라?
- 그냥 스파크자체가 sql이랑 뭐가달라?
- 마지막으로해보고싶은건?
  - 분산 파티셔닝 어떻게 할지, 셔플어떻게할지?
  - 좀 익숙해지면 대용량 처리, 실제 최적화 포인트 어딘지 찾아가기 등의 과제가 있으면 좋겠다
- 병렬처리가 잘되는지? 보려면 spark history server
- *"spark를 굳이쓰는게 맞는가"* 라는 고민도..
  - 그냥 python으로 데이터 처리하는거랑 다른거같지도않고
- dsl 의 장점
  - 디버깅의 편리성. 안드로이드같은 경우나 javask 이런거처럼...
  - string 형식의 sql보다 가독성도 좋고 일반 비즈니스로직처럼 가져갈수있으니까!
    - like kotlin (spark의 장점이기도. lambda식. ~식으로 질의를 하는게 편했다) + gradle도 kotlin을 권장하고있고...