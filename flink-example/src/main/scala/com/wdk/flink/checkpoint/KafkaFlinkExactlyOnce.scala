package com.wdk.flink.checkpoint

import java.util.{Locale, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /4/11 0011 15:42
  * @Version: v1.0
  **/
object KafkaFlinkExactlyOnce {
    def main(args: Array[String]): Unit = {
        //定义flink执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //定义kafkaSource
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"testCheckPoint")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ROOT))
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")

        val kafkaSource = new FlinkKafkaConsumer011[String]("test_0411_01",new SimpleStringSchema(),properties)

        val inputStream = env.addSource(kafkaSource).print("test_0411_01>>>")

        env.execute()
    }
}
