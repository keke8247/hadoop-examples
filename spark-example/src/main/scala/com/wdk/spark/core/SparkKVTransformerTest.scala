package com.wdk.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @Description
  *             key - value 算子测试
  * @Author rdkj
  * @CreatTime 2020/6/16 10:29
  * @Since version 1.0.0
  */
object SparkKVTransformerTest {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkKVTransformerTest")

        val sc: SparkContext = new SparkContext(sparkConf)

        val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1,"zhangsan"),(2,"lisi"),(3,"wangwu"),(4,"zhaoliu")),4)

        //test partitionBy
        val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new HashPartitioner(2))
//        rdd1.saveAsTextFile("spark-example/out/test4")
//        rdd2.saveAsTextFile("spark-example/out/test5")

        //test groupByKey 根据key分组.把value汇总成一个集合
        val rdd3: RDD[(String, Int)] = sc.makeRDD(List("hello","spark","hello","scala","hello","alibaba","huawei","tecent","baidu")).map((_,1))
        val groupByKeyRdd: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
        println("groupByKeyRdd~~~~~~~~~~~"+groupByKeyRdd.collect().mkString(","))
        val wcRDD: RDD[(String, Int)] = groupByKeyRdd.map {
            case (key, values) => {
                (key, values.sum)
            }
        }
        println("wcRDD~~~~~~~~~~~"+wcRDD.collect().mkString(","))

        //test reduceByKey 根据key聚合 传入方法对value进行操作.
        val reduceWCRDD: RDD[(String, Int)] = rdd3.reduceByKey(_+_)
        println("reduceWCRDD~~~~~~~~~~~"+reduceWCRDD.collect().mkString(","))

        //test aggregateByKey
        val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a",20),("b",14),("a",10),("c",11),("b",18),("a",12),("c",23),("d",12),("a",23)),2)
        val rdd5: RDD[(String, Int)] = sc.makeRDD(List(("a",20),("b",14),("a",10),("c",11),("b",18),("a",12),("c",23),("d",12),("a",23)),2)
        //取出每个分区内相同key的最大值.然后相加
        val aggRDD: RDD[(String, Int)] = rdd4.aggregateByKey(0)(Math.max(_,_),_+_)
        println("aggRDD~~~~~~~~~~~"+aggRDD.collect().mkString(","))

        //test foldByKey 简化版的aggregateByKey 分区内和分区间的函数相同.
        val foldRDD: RDD[(String, Int)] = rdd4.foldByKey(0)(_+_)
        println("foldRDD~~~~~~~~~~~"+foldRDD.collect().mkString(","))

        //test combineByKey 根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
        val combinerRdd: RDD[(String, (Int, Int))] = rdd4.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc2._2+acc2._2))
        println("combinerRdd~~~~~~~~~~~"+combinerRdd.collect().mkString(","))
        val avgRDD: RDD[(String, Int)] = combinerRdd.map {
            case (key, v) => (key, v._1 / v._2)
        }
        println("avgRDD~~~~~~~~~~~"+avgRDD.collect().mkString(","))

        //test sortByKey 按照Key的字典顺序倒叙排序
        val sortRDD: RDD[(String, Int)] = rdd4.sortByKey(false)
        println("sortRDD~~~~~~~~~~~"+sortRDD.collect().mkString(","))

        //test mapValues 只对value进行处理. value+1
        val mapValueRDD: RDD[(String, Int)] = rdd4.mapValues(_+1)
        println("mapValueRDD~~~~~~~~~~~"+mapValueRDD.collect().mkString(","))

        //test cogroup
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd4.cogroup(rdd5)
        println("cogroupRDD~~~~~~~~~~~"+cogroupRDD.collect().mkString(","))
    }
}
