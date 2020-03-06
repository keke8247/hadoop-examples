package com.wdk.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:
  *              spark 各类算子用法测试
  * @Author:wang_dk
  * @Date:2020 /3/3 0003 22:09
  * @Version: v1.0
  **/
object SparkTransformerTest {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformerTest")

        val sc = new SparkContext(sparkConf)

        //rdd的三种来源  1,从集合创建 2,从外部存储系统数据集 3,从其他RDD转换而来
        //1,从集合创建 makeRdd 可以通过第二个参数 指定分区数.如果不指定 根据local[N] 里面N 决定. 如果是* 则是机器CPU核数
        val makeRdd: RDD[Int] = sc.makeRDD(1 to 10)
        println(makeRdd.partitions.length)

        //2,从外部存储系统数据集
        val fileRdd: RDD[String] = sc.textFile("spark-example/in")
        println(fileRdd.collect().mkString(","))

        //3,从其他RDD转换而来 通过一些转换算子
        val transRdd = makeRdd.map(_*2)
        println(transRdd.collect().mkString(","))

        //mapPartitions 类似Map 但独立的在Rdd的每一个分区上运行.类似于每次处理一个partition的数据.批量处理的感觉 内存够大的话 性能很棒.
        val mapPartitionsRdd = transRdd.mapPartitions(datas => datas.map(_*2))

        //mapPartitionsWithIndex 类似于mapPartitions 但是 func 带有一个Int型参数表示 分区index
        transRdd.mapPartitionsWithIndex{
            case (index,datas) => {
                datas.map((_,index))
            }
        }.foreach(println)


        // sample 算子可以进行 数据采样. 参数1 false 表示采样数据不放回 true表示放回
        println(transRdd.sample(true, 0.6, 1).collect().mkString(","))

        val  rdd = sc.makeRDD(List(("a",1)))
        rdd.groupByKey()




    }

}
