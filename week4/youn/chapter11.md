# Chapter 11. Dataset

- Dataset: 구조적 API의 기본 데이터 타입
  - Dataframe: Row type의 DataSet.
  - Dataset을 이용해 Data의 각 row를 구성하는 객체를 정의 한다.
  - JVM을 사용하는 언어인 스칼라와 스파크에서만 사용 가능.
    - 스칼라: 스키마가 정의된 케이스 클래스 객체로 Dataset 정의
    - Java: Bean으로 DataSet 정의
- Encoder
  - 도메인 별 특정 데이터 타입 객체 T -> 스파크의 내부 데이터 타입으로 매핑하는 시스템
- 사용자가 정의한 데이터 타입도 분산 방식을 다룰 수 있다.
- 사용자가 정의한 데이터 타입
  - Row 포맷보다 성능은 나쁘지만, 유연성이 좋다.
- 사용자 정의 함수 (UDF)
  - 사용자 정의 데이터 타입 보다 성능이 나쁘다.

## 11.1 Dataset을 사용하는 경우

## 11.2 Dataset 생성

- Dataset을 정의하는 것은 수동작업, 정의할 스키마를 미리 알고 있어야 한다.

### 11.2.1 자바: Encoders

- 데이터 타입 클래스 정의한 후 \
  DataFrame(Datasete<Row> 타입)에 지정하여 인코딩 가능.

### 11.2.2 스칼라: 케이스 클래스

## 11.3 액션

## 11.4 트랜스포메이션

- DataFrame의 트랜스포메이션과 동일하다. \
  (i.e Dataframe의 모든 트랜스포메이션을 dataset에서 사용가능하다.)

#### 11.4.1 필터링

#### 11.4.2 매핑

## 11.5 조인

- DataFrame과 동일하게 적용.
- `joinWith`와 같은 더 정교한 메소드를 제공.
  - `joinWith`를 사용하지 않은 일반 join일 경우, dataframe 반환. \
    (i.e JVM 데이터 타입 정보를 잃어버림)
  - DataFrame과 Dataset을 조인하는 것은 문제가 없고, \
    **동일한 결과(?)**를 반환.

## 11.6 그룹화와 집계

- Dataframe과 동일하게 적용.
- `groupBy, rollup, cube` 사용이 가능하지만, **DataFrame** 반환.
- `GroupByKey`: function을 parameter로 받고, dataset을 반환.
  - 성능이 떨어지지만, function을 parameter로 받아서 유연성이 좋다(정교한 작업이 가능하다).
