package com.wdk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/21 0021 9:43
  * @Version: v1.0
  **/
object AggregateByKeyTest {
    def main(args: Array[String]): Unit = {
        val sparkconf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyTest")

        val spark = new SparkContext(sparkconf)

        val pairRdd = spark.makeRDD(Array(('a',29),('a',19),('b',19),('c',39),('c',20),('a',15)),2)

        println(pairRdd.partitions.size)


        //aggregateByKey 参数描述
        /*
        *   zreoValue : 0 初始值 就是第一个数据进来 和 初始值进行max比较  大的保留
        *   seqOp: math.max(_,_) 分区内函数 分区内所有相同key的 value 区最大值.
        *   combOp: _+_ 分区间函数 分区间相同key 的value 求和.
        */
        println(pairRdd.aggregateByKey(0)(math.max(_,_),_+_).collect().mkString(","))
    }
}
