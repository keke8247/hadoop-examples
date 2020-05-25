package com.wdk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/21 0021 9:59
  * @Version: v1.0
  **/
object CombineByKeyTest {
    def main(args: Array[String]): Unit = {
        val sparkconf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyTest")

        val spark = new SparkContext(sparkconf)

        val pairRdd = spark.makeRDD(Array(('a',29),('a',19),('b',19),('c',39),('c',20),('a',15)),2)

        println(pairRdd.partitions.size)


        //aggregateByKey 参数描述
        /*
        *   createCombiner : (_,1) 为key创建初始值 key第一次进入 用 value 创建 二元组 统计key出现次数
        *   mergeValue: (imem:(Int,Int),v)=>(imem._1+v,imem._2+1)  分区内函数 如果key值已经出现过 把value值相加  次数+1(对Value进行分区内聚合)
        *   mergeCombiners: (imem1:(Int,Int),imem2:(Int,Int)) =>(imem1._1+imem2._1,imem1._2+imem2._2) 分区间函数 分区间相同key 的value 求和,次数求和.
        */
        val combineRdd = pairRdd.combineByKey((_,1),
            (imem:(Int,Int),v)=>(imem._1+v,imem._2+1),
            (imem1:(Int,Int),imem2:(Int,Int)) =>(imem1._1+imem2._1,imem1._2+imem2._2))

        pairRdd.cogroup(pairRdd).foreach(item=>{
            println(item._2._2)
        })
//        println(combineRdd.map(item =>{
//            val avg = item._2._1/item._2._2
//            (item._1,avg)
//        }).collect().mkString(","))
    }
}
