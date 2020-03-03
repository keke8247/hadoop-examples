package com.wdk.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/3 14:52
  * @Since version 1.0.0
  */
object SparkWc {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Wc")

        //定义spark环境
        val sparkContext = new SparkContext(sparkConf)

        //第二个参数 可以设置并行度 指定最小分区数
        val inputData = sparkContext.textFile("spark-example/in",6)

//        inputData.saveAsTextFile("spark-example/output")

        val wordCount = inputData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

        wordCount.foreach(println)

    }
}
