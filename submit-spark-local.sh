#!/bin/bash
$SPARK_HOME/bin/spark-submit --class spark.test.WordCount --master local[8]  target/wordCount-jar-with-dependency.jar file:///home/wangke/workspace/logs/bednets/phish_1.3  file:///home/wangke/workspace/logs/loglog/output
