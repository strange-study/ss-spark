# Spark Architecture

## Spark 란?
* 대규모 데이터 처리를 위한 통합 분석 엔진, \
클러스터 환경에서 데이터를 병렬로 처리하는 라이브러리 집합.
* In-memory 연산 처리 엔진으로, \
데이터가 disk 대신 RAM에 저장되며 병렬 처리 된다.
* 스파크는 스파크 애플리케이션과 클러스터 매니저로 구성된다.
  - 스파크가 지원하는 클러스터 매니저 종류
    + 스파크 스탠드얼론 클러스터 매니저
    + 하둡 YARN
    + Mesos
  - 사용자가 클러스터 매니저에 스파크 애플리이션을 제출하면, \
  클러스터 매니저는 애플리케이션에 필요한 리소스를 할당한다.

## How spark runs on Clusters

### Spark Application

![Spark The Definitive Guide 참조](./SparkStructure.png)

* 하나의 드라이버(driver) 프로세스와 다수의 익스큐터(executor) 프로세스로 구성된다.
* 드라이버(Driver) 프로세스: 스파크 애플리케이션의 실행을 제어하며, main() 함수를 실행한다.
  - 전반적인 익스큐터 프로세스의 작업과 관련된 분석, 배포 그리고 스케줄링
    + 사용자 코드를 실제 수행 단위인 Task로 변환 해 익스큐터(Excutor)에 할당.
  - SparkContext 생성: 
* 익스큐터(Executor) 프로세스: 드라이버 프로세스가 할당한 작업(task)을 수행하며 결과를 전달한다.

### Cluster Manager
* 물리적인 머신에 연결되어 자원을 관리한다.
* master(or driver) - worker 구조를 가지고 있다.
  - cluster manager의 drvier process와 master는 별개의 개념이다.

### Spark Application 실행 흐름

![SparkComponents](./SparkComponents.png)


1. 사용자가 spark-submit을 이용해 애플리케이션 제출.
2. spark-submit이 드라이버 프로세스를 실행하고 \
사용자가 정의한 main() 메소드를 호출한다.
    - spark context도 생성되서 cluster manager와 연결.
3. driver process가 cluster manager에게 executor 실행을 위한 자원을 요청.
4. cluster manager가 driver에게 요청 받은 리소스 만큼 executor 자원을 할당한 후 실행.
5. driver를 통해 사용자 애플리케이션이 실행된다.
    - DAG Scheduling을 통해 드라이버는 작업을 task 단위로 분할하여 \
  executor에게 할당한다.
6. Executor 들은 단위 작업(task)를 수행한 후, \
driver process에게 다시 보낸다.
7. driver는 main()이 종료되거나, SparkContext.stop()이 호출 될 때까지 \
task 할당하는 작업을 반복하며, 종료될 때는 executor들을 중지시키고 \
cluster manager에게 사용했던 자원을 반납한다.


##### Deploy Mode
* 클러스터 사용 시 드라이버 프로세스의 실행 위치를 지정한다.
* Client mode: 드라이버가 클러스터 외부에 위치
  - 드라이버가 spark-submit의 일부로 실행
  - 드라이버 프로세스의 출력 결과 확인 가능
  - 애플리케이션이 실행되는 동안 작업 노드들에 계속 연결되어 있어야 한다. 
* Cluster mode: 드라이버가 클러스터 내부에 위치
  - 드라이버 프로세스가 작업 노드 중 하나에서 실행됨
  - 실행 후 개입되지 않는 방식

* 클러스터 매니저는 물리적 머신을 관리하고, 스파크 애플리케이션에 자원을 할당합니다.



## Spark의 다양한 언어 API
* 다양한 Spark 언어 API(Java, Scala, Python, R)를 통해 스파크 코드를 실행할 수 있다.
* 스파크는 파이썬이나 R로 작성한 코드를 익스큐터의 JVM에서 실행할 수 있는 코드로 변환합니다. \
(드라이버 프로세스에서 진행)

![figure2-2](./figure2-2.jpeg)


## Spark Architecture and Deploy Modes

* master-worker architecture


* deploy mode
  - local: master, executor, driver in the same single JVM machine
  - standalone: 
  - YARN, Mesos etc