owner=$1

echo $owner

if [ $owner = "sw" ]; then
    /home/ubuntu/spark-3.0.3-bin-hadoop2.7/bin/spark-submit \
        --class com.sw.test.ProjectApp \
        --master local[*] \
        ss-spark-1.0-SNAPSHOT-all.jar
fi

if [ $owner = "mk" ]; then
    spark-submit \
        --class DcTestApp \
        --master local[*] \
        sparkTest.jar
fi

if [ $owner = "jj" ]; then
    today=$(date +'%Y%m%d')
    spark-submit \
        --class analyze_dc_jj.AnalyzeDCApp \
        --master local[*] \
        --driver-memory 2G \
        --executor-memory 1G \
        analyze_dc-assembly-0.1.jar \
        $today
fi
