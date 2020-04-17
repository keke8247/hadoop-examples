package com.wdk.flink.window

import java.lang

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description
  *             15秒内最小的温度
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/4/9 10:29
  * @Since version 1.0.0
  */
object TestWindow {
    def main(args: Array[String]): Unit = {
        //定义flink执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream = env.readTextFile("F:\\myGitHub\\hadoop\\hadoop-examples\\flink-example\\src\\main\\resources\\sensor.txt")
            .map(item=>{
                val attr = item.split(",")
                SensorReading(attr(0).trim,attr(1).toLong,attr(2).toDouble)
            })

        val minTemperature = inputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
            override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
        }).keyBy(_.id)
            .timeWindow(Time.seconds(15))
                .reduce((s1,s2)=>SensorReading(s1.id,s2.timestamp,s1.temperature.min(s2.temperature)))
            .print()

        env.execute("取15秒最低温度值.")
    }
}

