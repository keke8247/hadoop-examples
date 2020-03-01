package com.wdk.flink.sink

import java.util.Properties

import com.wdk.flink.domain.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig


/**
  * @Description:
  *              1.使用flume监控日志文件 采集信息到kafka的topic_sensor
  *              2.kafkaSource 订阅topic_sensor 消费消息
  *              3.Flink对kafkaSource传递过来的数据进行处理 通过FlinkKafkaProducer011输出数据到kafka的 flink_kafka_sink topic中
  * @Author:wang_dk
  * @Date:2020 /2/29 0029 17:22
  * @Version: v1.0
  **/
object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //kafka sources
        val config = new Properties()
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092")
        config.put(ConsumerConfig.GROUP_ID_CONFIG,"kafka_sink_CG")
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

        //创建KafkaSource
        val kafkaSource = new FlinkKafkaConsumer011[String]("topic_sensor",new SimpleStringSchema(),config)

        //为执行环境添加Source为 kafkaSource
        val inputStream = env.addSource(kafkaSource).map(line => {
            val attr = line.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble).toString
        })

        inputStream.addSink(new FlinkKafkaProducer011[String]("master:9092,slave1:9092,slave2:9092","flink_kafka_sink",new SimpleStringSchema()))
        inputStream.print()

        env.execute("kafka sink test")

    }
}
