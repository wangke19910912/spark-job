package spark.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
   一个main函数中可以有多个rdd job,rdd job在执行
   一个job在调用Action提交任务后会转化成为多个stage
   一个stage会转化成为一组相同的task,作用到数据分片进行计算
 */
object OperatorTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("spark world count").setIfMissing("spark.master", "local[2]")

    val sc = new SparkContext(sparkConf)


    val rdd = sc.parallelize(Seq(0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9), 2);
    val rdd_s = sc.parallelize(Seq("this is line one", "we have line 2"))
    val rdd_kv = sc.parallelize(Seq(("a", "b"), ("a", "d"), ("e", "f"), ("h", "g")), 2)
    //map,对rdd中的每一个元素进行 _*3 操作,collect会将最终结果汇聚到driver上进行计算
    val map_rdd = rdd.map(_ * 3);
    val map_rdd_arr = map_rdd.collect()
    println(map_rdd_arr.toList)
    println("Test A /////////////////////////////////////////////////")


    //mapPartations mapPartation会对每个分区执行操作,
    val map_partitions_rdd = rdd.mapPartitions(it => {

      it.foreach(println _)
      it.foreach(_ * 2)
      println("partition is over")
      it
    })

    val map_partition_rdd_arr = map_partitions_rdd.collect();
    println(map_partition_rdd_arr.toList)
    println("Test B ////////////////////////////////////////////////")


    //flatMap flatMap先会进行flat操作, 之后会进行flat操作, 将集合转化为一个
    val flatMap_rdd = rdd_s.flatMap(_.split(" "))
    val flatMap_rdd_arr = flatMap_rdd.collect()
    println(flatMap_rdd_arr.toList)
    println("Test C ////////////////////////////////////////////////")


    //filter filter会将不满足条件的记录进行过滤操作
    val filterMap_rdd = rdd.filter(_ > 3)
    val filterMap_rdd_arr = filterMap_rdd.collect()
    println(filterMap_rdd_arr.toList)
    println("Test D ///////////////////////////////////////////////")


    //distinct 将重复的元素去除掉
    val distinct_rdd = rdd.distinct()
    val distinct_rdd_arr = distinct_rdd.collect()
    println(distinct_rdd_arr.toList)
    println("Test E ////////////////////////////////////////////////")


    //cartesian 求出笛卡尔积,用于将key类型转化为key-value类型
    val caresian_rdd = rdd.cartesian(rdd_s)
    val caresian_rdd_arr = caresian_rdd.collect()
    println(caresian_rdd_arr.toList)
    println("Test F ////////////////////////////////////////////////")


    //union 将两个rdd进行合并,1.保证数据类型一致2.不会进行去重
    val union_rdd = rdd.union(rdd)
    val union_rdd_arr = union_rdd.collect()
    println(union_rdd_arr.toList)
    println("Test G ////////////////////////////////////////////////")


    //mapValues 对k-v类型的,函数对value进行作用
    val mapValues_rdd = rdd_kv.mapValues(_ + "tail")
    val mapValues_rdd_arr = mapValues_rdd.collect()
    println(mapValues_rdd_arr.toList)
    println("Test H ////////////////////////////////////////////////")

    //groupbyKey,会将rdd重新shuffer,将key相同数据分到一组,两种书写形式

    val groupValues_rdd = rdd_kv.groupByKey.map {
      case item: (String, Iterable[String]) => {

        val key = item._1
        val it = item._2

        var total = ""

        for (i <- it) {
          total = i + total
        }

        (key, total)
      }
    }
    val groupValues_rdd_arr = groupValues_rdd.collect()
    println(groupValues_rdd_arr.toList)
    println("Test I ////////////////////////////////////////////////")

    val groupValues_rdd2 = rdd_kv.groupByKey.map(
      (item: (String, Iterable[String])) => {
        val key = item._1
        val it = item._2

        var total = ""

        for (i <- it) {
          total = i + total
        }

        (key, total)
      });

    val groupValues_rdd_arr2 = groupValues_rdd2.collect()
    println(groupValues_rdd_arr2.toList)
    println("Test J ///////////////////////////////////////////////")


    //reduceBykey,将key相同的一组进行合并,内部调用combineByKey
    val reduceKey_rdd = rdd_kv.reduceByKey((value1: String, value2: String) => {

      value1 + value2
    })

    val reduceKey_rdd_arr = reduceKey_rdd.collect()
    println(reduceKey_rdd_arr.toList)
    println("Test K //////////////////////////////////////////////")


    //join
    val join_rdd = rdd_kv.join(rdd_kv)
    join_rdd.map((item: (String, (String, String))) => {

      ""
    })
    val join_rdd_arr = join_rdd.collect()
    println(join_rdd_arr.toList)
    println("Test L /////////////////////////////////////////////")


    //paratationBy,会将rdd根据分区函数进行重新分区操作
    //cache,persist,将中间结果缓存到内存中,避免多次计算
    join_rdd.persist(StorageLevel.DISK_ONLY)
    join_rdd.cache()

    //Action
    //Action 算子会将结果运算后归结到Driver端

    //checkpoint 避免缓存丢失带来性能开销
    join_rdd.checkpoint()
  }


}
