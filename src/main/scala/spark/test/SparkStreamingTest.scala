package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by wangke on 15-12-18.
 */

/*
   spark streaming 将输入按时间分片进行计算,每个时间分片会生成一个task
   spark streaming 可以接受的输入源有hdfs,kafka,socket等
 */
object SparkStreamingTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)



  }

}
