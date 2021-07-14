### 빌드 (Fat Jar)

```bash
$ gradle shadowJar
# => output : build/libs/ss-spark-1.0-SNAPSHOT-all.jar
```


### 환경
> Spark 3.0.3 + Scala 2.12.13 + Gradle Build
> on Docker (Local)

spark-submit 시 scala 버전 충돌 때문에 scala 2.12 를 사용하는 Spark 3.x 사용

```bash
# Submitting Application (spark-submit)
./spark-3.0.3-bin-hadoop2.7/bin/spark-submit --class "com.sw.test.ProjectApp" --master local ss-spark-1.0-SNAPSHOT-all.jar
```

- Output : [/output](./output)
