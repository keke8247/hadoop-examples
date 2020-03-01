package com.wdk.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.wdk.flink.domain.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig



/**
  * @Description:
  *              构建自定义JDBCSink 把flink处理的结果数据写入关系型数据库
  * @Author:wang_dk
  * @Date:2020 /2/29 0029 21:05
  * @Version: v1.0
  **/
object JdbcSinkTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //构建KafkaSource
        val config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092")
        config.put(ConsumerConfig.GROUP_ID_CONFIG,"jdbc_sink")
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

        val kafkaSource = new FlinkKafkaConsumer011[String]("topic_sensor",new SimpleStringSchema(),config)
        //transform
        val inputhStream = env.addSource(kafkaSource).map(item=>{
            val attr = item.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        })

        //sink
        inputhStream.addSink(new MyJdbcSink())

        env.execute("jdbc sink test")
    }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
    var conn : Connection = null;
    var INSERT : PreparedStatement = null;
    var UPDATE : PreparedStatement = null;

    //open的时候创建连接 预编译语句
    override def open(parameters: Configuration) = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","password");
        INSERT = conn.prepareStatement("insert into temperature (sensor,temp) values (?,?)")
        UPDATE = conn.prepareStatement("update temperature set temp = ? where sensor = ?")
    }

    //执行业务逻辑
    override def invoke(sensorReading: SensorReading, context: SinkFunction.Context[_]) = {
        //1. 执行Update语句
        //构建参数
        UPDATE.setDouble(1,sensorReading.temperature)
        UPDATE.setString(2,sensorReading.id)
        UPDATE.execute()

        if(UPDATE.getUpdateCount == 0){ //更新时候 没有更新到数据 说明没有该Id  改为Insert语句
            INSERT.setString(1,sensorReading.id)
            INSERT.setDouble(2,sensorReading.temperature)
            INSERT.execute()
        }
    }

    //释放资源
    override def close() = {
        super.close()
        UPDATE.close()
        INSERT.close()
        conn.close()
    }
}
