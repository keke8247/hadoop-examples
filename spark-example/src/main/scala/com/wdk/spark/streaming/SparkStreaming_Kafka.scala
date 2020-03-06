package com.wdk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @Description:
  *              SparkStreaming 使用kafka作为数据输入源
  * @Author:wang_dk
  * @Date:2020 /3/6 0006 21:14
  * @Version: v1.0
  **/
object SparkStreaming_Kafka {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Kafka")

        //声明sparkStreaming 上下文环境 数据采集周期为10秒
        val streamContex = new StreamingContext(sparkConf,Seconds(10))


        val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamContex,
            "master:2181",
            "spark_streaming_consumer",
            Map("spark_streaming" -> 2))


        kafkaDStream.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).print()

        streamContex.start()

        streamContex.awaitTermination()
    }

}
