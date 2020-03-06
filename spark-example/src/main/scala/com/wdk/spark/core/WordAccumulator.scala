package com.wdk.spark.core


import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description
  *             自定义累加器
  *             累加器:分布式只写共享变量
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/5 14:53
  * @Since version 1.0.0
  */
object WordAccumulator {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordAccumulator-test")

        val sc = new SparkContext(sparkConf)

        val dataRdd : RDD[Int]= sc.makeRDD(List(1,2,3,4),2)

        //求 dataRdd里面所有元素的和
        println(dataRdd.reduce(_ + _))


        //这样处理是不行的, 因为sum 是定义在Driver里面.
        // 而foreach计算实在Executor里面进行的
        // 把sum传到executor 并没有传回来.输出的时候输出的是Driver里面的 所以 永远是0
        /*
        var sum:Int = 0;
        dataRdd.foreach{
            i => {
                sum = sum+i
            }
        }
        println(sum)
        */

        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        //使用一个累加器
        val accumulator : LongAccumulator = sc.longAccumulator
        dataRdd.foreach{
            i => {
                accumulator.add(i)
            }
        }
        println(accumulator.value)


        //自定义一个累加器,把单词里面含"h"的单词返回

        val wordRdd = sc.makeRDD(List("hive","hbase","spark","hadoop","scala","flink"))

        //声明累加器
        val wordAccumulator = new MyWordAccumulator();
        //注册累加器
        sc.register(wordAccumulator)
        wordRdd.foreach(word=>{
            wordAccumulator.add(word)
        })

        println(wordAccumulator.value)

        sc.stop()
    }

}

class MyWordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{
    var listStr : util.ArrayList[String] = new util.ArrayList[String]()

    override def isZero: Boolean = listStr.isEmpty

    //复制累加器
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
        new MyWordAccumulator()
    }

    override def reset(): Unit = {
        listStr.clear()
    }

    override def add(v: String): Unit = {
        if(v.contains("h")){
            listStr.add(v)
        }
    }

    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
        listStr.addAll(other.value)
    }

    override def value: util.ArrayList[String] = listStr
}
