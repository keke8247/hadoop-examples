package com.wdk.flink.sink

import java.util.Properties

import com.wdk.flink.domain.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @Description:
  *              1.使用flume监控日志文件 采集信息到kafka的topic_sensor
  *              2.kafkaSource 订阅topic_sensor 消费消息
  *              3.Flink对kafkaSource传递过来的数据进行处理 RedisSink 把结果写入到Redis
  * @Author:wang_dk
  * @Date:2020 /2/29 0029 18:12
  * @Version: v1.0
  **/
object RedisSinkTest {
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
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        })

        val redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build()

        inputStream.addSink(new RedisSink(redisConf,new MyRedisMapper()))

        env.execute("flink redis sink")

    }

}

case class MyRedisMapper() extends RedisMapper[SensorReading]{

    //定义写入到Redis 执行的命令
    override def getCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
    }

    override def getValueFromData(data: SensorReading) = {
        data.temperature.toString
    }

    override def getKeyFromData(data: SensorReading) = {
        data.id
    }
}
