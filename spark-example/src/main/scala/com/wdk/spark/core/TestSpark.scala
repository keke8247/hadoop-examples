package com.wdk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/20 9:23
  * @Since version 1.0.0
  */
object TestSpark {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TestSparkCore")

        val sc = new SparkContext(sparkConf)

//        val rdd = sc.makeRDD(1 to 16)
//
//        rdd.glom().collect().foreach(arr =>{
//            println(arr.mkString("/"))
//        })
//
//        rdd.repartition(2)
//
//        println(rdd.partitions.size)


        val rdd1 = sc.parallelize(1 to 5)
        val rdd2 = sc.parallelize(3 to 8)
//        println(rdd1.subtract(rdd2).collect().mkString("/"))  //返回rdd1 与 rdd2 的差集 即 rdd1 有 rdd2没有的元素



        println(rdd1.aggregate(0)(math.max(_,_),_+_))

//        rdd2.map((_,1)).aggregateByKey()

    }
}
