# 6주차 정리

## 13장
- 질문 -> flatMap과 Map의 차이는???!!!
- groupByKey : 모든 익스큐터에서 함수를 적용하기 전에 해당 키와 관련된 모든 값을 메모리로 읽어 들여야 한다는 이슈가 있음 이로 인하여 치우쳐진 키가 있다면 일부 파티션이 엄청난 양의 값을 가질 수 있으므로 OOM 발생 여지가 있음.
- reduceByKey : flatMap, Map을 수행한 이후에 결괏값을 합계함수와 함께 reduceByKey 메서드를 수행 -> 각 파티션에서 리듀스 작업을 수행하기 때문에 훨씬 안정적 -> 모든 값을 메모리에 유지하지 않아도 됨. -> 작업 부하를 줄이려는 경우에 적합 ( 개별 요소들은 정렬되어 있지 않음.)
- aggregate 첫 번째는 파티션 내에서 수행되고 두 번째 함수는 모든 파티션에 걸쳐 수행됨 -> 익스큐터끼리 트리를 형성해 집계 처리의 일부 하위 과정을 푸시다운 방식으로 먼저 수행시켜서 oom을 피할 수 있게하는 treeAggregate가 있다.
- coalesce 데이터 셔플링 없이 하나의 파티션으로 합칠 수 있다.
- repartition 을 통하여 파티션 수를 늘릴 수 있다. 노드 간의 셔플이 발생할 수 있다.
- 사용자 정의 파티셔닝의 유일한 목표는 데이터 치우침 같은 문제를 피하고자 클러스터 전체에 걸쳐 데이터를 균등하게 분배하는 것
- kayo 직렬화를 통하여 직렬화 속도를 향상시킬 수 있다.(rdd를 셔플링하면 내부적으로 kryo 시리얼라이저 사용)