package spark.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
   一个main函数中可以有多个rdd job,rdd job在执行


 */
object WordCount {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input> <output>")
      System.exit(1)
    }

    val Array(input, output) = args

    val sparkConf = new SparkConf().setAppName("spark world count").setIfMissing("spark.master", "local[2]")

    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile(input);

    val result = lines.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)

    result.saveAsTextFile(output)
    sc.stop()

  }


}
