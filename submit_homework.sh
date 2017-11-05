#!/usr/bin/env bash
TEST_RESOURCES='/spark-core/spark-sql/spark-sql-homework1/src/test/resources/integration/input/'
hdfs dfs -rm -r -skipTrash $TEST_RESOURCES
hdfs dfs -mkdir -p $TEST_RESOURCES
hdfs dfs -copyFromLocal $TEST_RESOURCES $(dirname $TEST_RESOURCES)
hdfs dfs -rm -r -skipTrash /results
/usr/hdp/current/spark-client/bin/spark-submit \
    --conf spark.ui.port=10500 \
    --jars jars/spark-csv_2.11-1.5.0.jar \
    --class com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation \
    target/spark-sql-hw.jar \
    $TEST_RESOURCES/bids.gz.parquet \
    $TEST_RESOURCES/motels.gz.parquet \
    $TEST_RESOURCES/exchange_rate.txt \
    /results
hdfs dfs -cat /results/aggregated/* | head -100