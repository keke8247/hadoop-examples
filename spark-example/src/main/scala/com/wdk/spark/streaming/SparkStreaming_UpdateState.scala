package com.wdk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 17:06
  * @Since version 1.0.0
  */
object SparkStreaming_UpdateState {
    def main(args: Array[String]): Unit = {
        //Spark环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_WC")

        //构建SparkStreaming执行环境 数据采集周期为10秒
        val streamingContext = new StreamingContext(sparkConf,Seconds(10))
        //要保留前一个采集周期的状态 就要设置checkpoint
        streamingContext.checkpoint("spark-example/cp")


        // 从socket 收集数据
        val streamingDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("master",9999)

//        val wordcountsDStream = streamingDStream.flatMap(line=>line.split(" ")).map((_,1)).reduceByKey(_+_)

        val mapDStream: DStream[(String, Int)] = streamingDStream.flatMap(line => line.split(" ")).map((_, 1))

        val wordcountDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            //updateStateByKey方法参数 是一个匿名函数
            // 匿名函数参数是 Seq[Int] 就是word 出现的次数 根据word聚合后 次数的列表List(1,1,1,1,....)
            // buffer 是做计算时候的缓冲区. 使用Option 避免空指针.
            case (count, buffer) => {
                val sum = buffer.getOrElse(0) + count.sum
                Option(sum)
            }
        }

        wordcountDStream.print()

        //启动采集器
        streamingContext.start()
        //等采集器停止 程序才停止
        streamingContext.awaitTermination()
    }
}
