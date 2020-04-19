package com.wdk.flink.checkpoint

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.requests.IsolationLevel

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /4/11 0011 13:21
  * @Version: v1.0
  **/
object TestCheckpoint {
    def main(args: Array[String]): Unit = {
        //定义flink执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStateBackend(new FsStateBackend("file:///D:\\files\\program\\idea\\hadoop-examples\\flink-example\\ck")) //设置状态后端. 状态的存储级别  memory/file/rocksDb

//        设置checkpoint的 间隔 和 级别.
        env.enableCheckpointing(6000,CheckpointingMode.EXACTLY_ONCE)

        //定义kafkaSource
        val properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"testCheckPoint")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")

        val kafkaSource = new FlinkKafkaConsumer011[String]("test_0411",new SimpleStringSchema(),properties)

        val inputStream = env.addSource(kafkaSource)
        inputStream.print("kafkaSource>>>>")

        //定义一个kafka sink
        val producerProperties = new Properties()
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
//        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
//        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

        val kafkaSink = new FlinkKafkaProducer011[String]("test_0411_01", new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
                                  producerProperties,Semantic.EXACTLY_ONCE)
//        val kafkaSink = new FlinkKafkaProducer011[String]("master:9092","test_0411_01",new SimpleStringSchema())
        inputStream.addSink(kafkaSink)

        env.execute("测试checkpoint")
    }


    //kafka实现 exactly-once  通过2pc的方式提交  / 在kafka sink哪里 指定 一致性级别为 exactly-once
    //kafka 消费数据时候隔离性 配置为 读已提交 数据 read-on-commit
    //kafka 的事务超时时长 > flink checkpoint 超时时长.
}
