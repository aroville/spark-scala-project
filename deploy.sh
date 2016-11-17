#!/usr/bin/env bash
sbt assembly && bash /opt/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.JobML --master spark://axel-roville-pc:7077 /home/axel/dev/introduction_au_framework_hadoop/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar
