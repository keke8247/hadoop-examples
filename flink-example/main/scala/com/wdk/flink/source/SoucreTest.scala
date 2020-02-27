package com.wdk.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @Description
  *             测试flink的 几种source
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/27 18:29
  * @Since version 1.0.0
  */
object SoucreTest {

    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //1 把已有集合作为source来源
        val source1 = env.fromCollection(List(
            SensorReading("sensor_1",System.currentTimeMillis(),3.0230492342),
            SensorReading("sensor_2",System.currentTimeMillis(),3.0230492342),
            SensorReading("sensor_3",System.currentTimeMillis(),3.0230492342),
            SensorReading("sensor_4",System.currentTimeMillis(),3.0230492342),
            SensorReading("sensor_5",System.currentTimeMillis(),3.0230492342),
            SensorReading("sensor_6",System.currentTimeMillis(),3.0230492342)
            )
        )
//        source1.print("source1").setParallelism(1)

        //2 通过文件
//        val source_file = env.readTextFile("filePath")

        //3 通过socket
//        val source_socket = env.socketTextStream("master",7777)

        //4 通过kafka  需引入kafka连接器 选择对应版本的FlinkKafkaConsumer
        val properties = new Properties();  //kafka 连接配置
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092")
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"sensor_consumer")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
        val source_kafka = env.addSource(new FlinkKafkaConsumer011[String]("topic_sensor",new SimpleStringSchema(),properties))
        source_kafka.print("source_kafka").setParallelism(1)

        env.execute()
    }

}

case class SensorReading( id: String, timestamp: Long, temperature: Double );
