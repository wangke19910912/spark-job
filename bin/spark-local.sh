#!/bin/bash
$SPARK_HOME/bin/spark-submit --class spark.test.WordCount --master local[8]  ../target/wordCount-jar-with-dependency.jar  file://$PWD/../input.dat  file://$PWD/../output.dat

