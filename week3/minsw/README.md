<!-- 
/ss-spark/week{#}/minsw/README.md

# Week {#}

## What I've Learned 🙂

## On/Offline
> 2021.00.00

-->


# Week 3


## What I've Learned 🙂
[ Part 2 구조적 API: DataFrame, SQL, Part 2Dataset ]

- CHAPTER 8 조인
  - [블로그](https://minsw.github.io/2021/02/15/Spark-The-Definitive-Guide-8%EC%9E%A5/)
- CHAPTER 9 데이터소스
  - [블로그](https://minsw.github.io/2021/02/16/Spark-The-Definitive-Guide-9%EC%9E%A5/)


## Online (Zoom)
> 2021.02.17

<img width="500" alt="week3" src="https://user-images.githubusercontent.com/26691216/108625048-026eee80-748c-11eb-8830-3ded66c2bdcc.png">


- 전반적으로 수월하게 이해한 챕터
  - 데이터소스의 SQL 데이터베이스 => SQLite때문에 예제 거의 패스
- minsw
  - 비슷하고 반복되는 구성. 이해하기 쉬움
  - 스파크 조인 수행방식 설명 중 큰테이블과 작은테이블은 기준이 애매해서..
- jin5335
  - 조인의 방식은 결국 셔플일 수 밖에 없는지?
  - 버켓팅 vs 파티셔닝 차이를 알수있어서 좋았다
    - 파티셔닝 => 디렉토리가 생기니까
    - 버켓팅 => 값의 종류가 많다고 하면 좀더모아서 처리하고자할 때 (파티션을 주고싶은데, 모든 값에대해 디렉토리를 생성하는게 부담스러울때 사용. 같은 물리적 파티션 안에 사용될 수 있게)
      - 파티셔닝보다 좀 더 작은 단위
- myeongki
  - p.239 해결방법1 이 잘 이해가안된다
    - A.id, B.id가 존재할때, 둘다유지되게되서 select()시 누구를 데려올지몰라서 에러가 발생
    - => 하나를 select()로 명시적으로 지정해버리는 방법 (=> 합쳐버리는 것)
    - 나머지는 빼거나 남기는거같은데 얘는 안그래서 헷갈렸다
  - p.242 "브로드캐스트의 경우 CPU가 가장 큰 병목 구간이다"
    - 셔플조인의 경우 연산 + 네트워크 부하등도 드는데
    - 브로드캐스트의 경우 첫 copy 이후에는 연산 부하 (CPU) 만 영향
- parquet + gzip 을 추천했는데, 실제로 성능차이가 있는지? 실무에서도 이렇게 사용하는지?
  - jin5335 : 기본적으로는 parquet를, 어디 넘겨줘야하는 needs가 있을땐 csv 사용 (단, 스키마를 명확히 할 필요 없을 때)
  	- "개인적으로는 parquet나 ORC처럼 스키마가 내장되어있는게 좋을 것 같다"
  - 옵션이 많을수록 다음번에 참조해야할 값이 많다는 이야기니까..
- p.269 슬라이딩 윈도우 기반의 파티셔닝
  - "최젓값에서 최곳값까지 동일하게 분배합니다" => 랜덤리? 적절히? => 예제돌려봐야할듯... (다음주에 추가적으로 얘기해봐요)