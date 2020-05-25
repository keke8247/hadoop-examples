package com.wdk.spark.core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/21 0021 9:55
  * @Version: v1.0
  **/
object FoldByKeyTest {
    def main(args: Array[String]): Unit = {
        val sparkconf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyTest")

        val spark = new SparkContext(sparkconf)

        val pairRdd = spark.makeRDD(Array(('a',29),('a',19),('b',19),('c',39),('c',20),('a',15)),2)

        println(pairRdd.partitions.size)


        //foldByKey 是aggregateByKey的简化版本  即:seqOp 和 combOp 分区内 分区间函数一致的情况
        /*
        *   zreoValue : 0 初始值 就是第一个数据进来 和 初始值进行max比较  大的保留
        *   func: _+_ 相同key 的value 求和.
        */
        println(pairRdd.foldByKey(0)(_+_).collect().mkString(","))
    }
}
