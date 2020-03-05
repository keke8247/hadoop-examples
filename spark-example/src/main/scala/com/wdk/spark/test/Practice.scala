package com.wdk.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/4 15:53
  * @Since version 1.0.0
  */
object Practice {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")

        val sc = new SparkContext(sparkConf)

        val input = "spark-example/in/agent.log"

        val dataRdd = sc.textFile(input).map(line=>{
            val attr = line.split(" ")
            DataInfo(attr(0).trim.toLong,attr(1).trim.toInt,attr(2).trim.toInt,attr(3).trim.toInt,attr(4).trim.toInt)
        })

        //需求：统计出每一个省份广告被点击次数的TOP3
        val top3 = dataRdd.map(info=>((info.province,info.advertisement),1)).reduceByKey(_+_).map{
            case(pa,num)=>(pa._1,(pa._2,num))
        }.groupByKey().mapValues(x=>x.toList.sortWith((x,y)=>x._2>y._2).take(3))
        top3.foreach(println)
    }
}

case class DataInfo(timestamp:Long,province:Int,city:Int,user:Int,advertisement:Int)