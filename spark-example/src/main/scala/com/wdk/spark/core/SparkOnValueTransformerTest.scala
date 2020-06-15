package com.wdk.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author rdkj
  * @CreatTime 2020/6/15 16:51
  * @Since version 1.0.0
  */
object SparkOnValueTransformerTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Test").setMaster("local[*]")

        val sparkContext = new SparkContext(conf)

        //makeRdd
        val rdd = sparkContext.makeRDD(1 to 10,2);

//        rdd.saveAsTextFile("spark-example/out/test")

        //test map
        val mapRdd: RDD[Int] = rdd.map(_*2)
        println("mapRDD~~~~~~~~~~~"+mapRdd.collect().mkString(","))

        //test mapPartitions  把一个partition的数据作为一个可迭代的Iterator 进行批处理
        val mapPartitionRDD : RDD[Int] = rdd.mapPartitions(x =>
            x.map(_ * 2) //这里的map 是scala的map 并不是RDD的map算子
        )
        println("mapPartitionRDD~~~~~~~~~~~"+mapPartitionRDD.collect().mkString(","))

        //test mapPartitionsWithIndex
        val mapPWIRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
            // index 是分区号. datas是分区内数据
            case (index, datas) => {
                datas.map((_, index)) //这里的map 是scala的map 并不是RDD的map算子
            }
        }
        println("mapPWIRDD~~~~~~~~~~~"+mapPWIRDD.collect().mkString(","))

        // test flatMap 打平处理.
        val rdd2 = sparkContext.makeRDD(Array(List(1,2,3),List(4,5,6)))
        println("rdd2~~~~~~~~~~~"+rdd2.collect().mkString(","))
        val flatMapRDD: RDD[Int] = rdd2.flatMap(x => x)
        println("flatMapRDD~~~~~~~~~~~"+flatMapRDD.collect().mkString(","))

        //test glom 将每一个分区形成一个数组.
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        println("glomRDD~~~~~~~~~~~"+glomRDD.collect().mkString(","))

        //test groupBy 根据计算结果分组
        val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
        println("groupRdd~~~~~~~~~~~"+groupRdd.collect().mkString(","))

        //test filter
        val filterRdd: RDD[Int] = rdd.filter(_%2==0)
        println("filterRdd~~~~~~~~~~~"+filterRdd.collect().mkString(","))

        //test sample 抽样  第一个参数表示 是否放回.
        val sampleRdd: RDD[Int] = rdd.sample(false,0.4,1)
        println("sampleRdd~~~~~~~~~~~"+sampleRdd.collect().mkString(","))

        //test distinct() / distinct(numPartitions) 可以传入分区数.去重之后可以缩减分区.
        val rdd3: RDD[Int] = sparkContext.makeRDD(List(1,2,4,3,2,4,5,6,7))
        val distinctRdd: RDD[Int] = rdd3.distinct()
        println("distinctRdd~~~~~~~~~~~"+distinctRdd.collect().mkString(","))

        //test
        // coalesce 合并分区. 如果当前两个分区. 合并分区改成4个  并不会新增分区.   coalesce 不会shuffle 只是把缩减掉的分区数据都放到最后一个分区.
        // repartition 重分区 如果当前两个分区. 分区改成4个  会新增两个分区. repartition 会进行shuffle 底层调用的就是 coalesce,shuffle=true
//        rdd.coalesce(1).saveAsTextFile("spark-example/out/test1")
//        rdd.repartition(3).saveAsTextFile("spark-example/out/test3")

        //test sortBy 根据本身进行排序 传false 倒叙 默认正序
        val sortRdd: RDD[Int] = distinctRdd.sortBy(x=>x,false)
        println("sortRdd~~~~~~~~~~~"+sortRdd.collect().mkString(","))

    }
}
