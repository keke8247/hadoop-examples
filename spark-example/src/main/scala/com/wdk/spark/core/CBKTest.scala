package com.wdk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  * @Author rdkj
  * @CreatTime 2020/5/27 9:50
  * @Since version 1.0.0
  */
object CBKTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("testCBK").setMaster("local[*]")

        val spark = new SparkContext(conf)

        val input = spark.makeRDD(Array(("a",30),("b",40),("c",30),("a",20),("b",40),("c",20)),2)

        input.combineByKey((_,1),
            (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),
            (p1:(Int,Int),p2:(Int,Int))=>(p1._1+p2._1,p1._2+p2._2)).map{
            case (key:String,(sum:Int,count:Int)) =>{
                (key,sum/count)
            }
        }.collect().foreach(println)
    }
}
