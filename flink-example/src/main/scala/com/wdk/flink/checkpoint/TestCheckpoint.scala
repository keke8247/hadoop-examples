package com.wdk.flink.checkpoint

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

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

        val checkpointConfig: CheckpointConfig = env.getCheckpointConfig

        //设置checkpoint的保留策略 任务取消时  保留checkpoint状态文件.
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        //设置两次 checkpoint 之间最小的时间间隔.
        /**当 Checkpoint 时间比设置的 Checkpoint 间隔时间要长时可以设置 Checkpoint 间最小时间间隔。
            这样在上次 Checkpoint 完成时，不会立马进行下一次 Checkpoint，而是会等待一个最小时间间隔，之后再进行 Checkpoint。
            否则，每次 Checkpoint 完成时，就会立马开始下一次 Checkpoint，系统会有很多资源消耗 Checkpoint 方面，而真正任务计算的资源就会变少。
          */
        checkpointConfig.setMinPauseBetweenCheckpoints(10000)


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
