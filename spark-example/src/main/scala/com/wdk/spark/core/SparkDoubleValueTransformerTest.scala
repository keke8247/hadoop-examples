package com.wdk.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  *             双Value算子操作
  * @Author rdkj
  * @CreatTime 2020/6/15 17:48
  * @Since version 1.0.0
  */
object SparkDoubleValueTransformerTest {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkDoubleValueTransformerTest").setMaster("local[*]")

        val sc: SparkContext = new SparkContext(sparkConf)

        val rdd1: RDD[Int] = sc.makeRDD(1 to 5)

        val rdd2: RDD[Int] = sc.makeRDD(3 to 7)

        //test union  并集 合并两个RDD的所有数据
        val unionRDD: RDD[Int] = rdd1.union(rdd2)
        println("unionRDD~~~~~~~~~~~"+unionRDD.collect().mkString(","))

        //test subtract 差集  rdd1.subtract(rdd2) 保留 rdd1中  rdd2不存在的数据.
        val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
        val subtractRDD2: RDD[Int] = rdd2.subtract(rdd1)
        println("subtractRDD~~~~~~~~~~~"+subtractRDD.collect().mkString(","))
        println("subtractRDD2~~~~~~~~~~~"+subtractRDD2.collect().mkString(","))

        //test intersection 交集
        val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
        println("intersectionRDD~~~~~~~~~~~"+intersectionRDD.collect().mkString(","))

        //test cartesian 笛卡尔积 (尽量避免使用 cartesian)
        val cartesianRDD: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
        println("cartesianRDD~~~~~~~~~~~"+cartesianRDD.collect().mkString(","))

        //test zip 拉链
        val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println("zipRDD~~~~~~~~~~~"+zipRDD.collect().mkString(","))
    }
}
