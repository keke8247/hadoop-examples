package com.wdk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 17:59
  * @Since version 1.0.0
  */
object SparkStreaming_Window {
    def main(args: Array[String]): Unit = {
        //Spark环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_WC")

        //构建SparkStreaming执行环境 数据采集周期为10秒
        val streamingContext = new StreamingContext(sparkConf,Seconds(3))

        // 从socket 收集数据
        val streamingDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("master",9999)

        //在DStream上 开窗口, 窗口大小必须是采集周期的整数倍, 滑动步长也是采集周期的整数倍,不然会报错.
        val windowDStream = streamingDStream.window(Seconds(9),Seconds(3))

        val wordcountsDStream = windowDStream.flatMap(line=>line.split(" ")).map((_,1)).reduceByKey(_+_)

        wordcountsDStream.print()

        //启动采集器
        streamingContext.start()
        //等采集器停止 程序才停止
        streamingContext.awaitTermination()
    }
}
