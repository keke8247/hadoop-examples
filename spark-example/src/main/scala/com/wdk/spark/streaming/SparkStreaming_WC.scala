package com.wdk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Description
  *             SparkStreaming_wordcount
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 15:33
  * @Since version 1.0.0
  */
object SparkStreaming_WC {
    def main(args: Array[String]): Unit = {
        //Spark环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_WC")

        //构建SparkStreaming执行环境 数据采集周期为10秒
        val streamingContext = new StreamingContext(sparkConf,Seconds(10))

        // 从socket 收集数据
        val streamingDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("master",9999)
        // streamingContext.receiverStream("添加自定义的Receiver")

        val wordcountsDStream = streamingDStream.flatMap(line=>line.split(" ")).map((_,1)).reduceByKey(_+_)

        wordcountsDStream.print()

        //启动采集器
        streamingContext.start()
        //等采集器停止 程序才停止
        streamingContext.awaitTermination()
    }
}
