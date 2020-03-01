package com.wdk.flink.window

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Description:
  *              时间窗口测试
  *              指定StreamTimeCharacteristic 为 EventTime
  * @Author:wang_dk
  * @Date:2020 /3/1 0001 14:07
  * @Version: v1.0
  **/
object TimeWindowTest_EventTime_TimeCharacteristic{

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.getConfig.setAutoWatermarkInterval(500) //500毫秒获取一次 watermark

        //Source
        val input = env.socketTextStream("master",7777).map(line => {
            val attr = line.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//            override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
//        })
                .assignTimestampsAndWatermarks(new MyAssigner())


        val windowedStream = input.map(item => {
            (item.id,item.temperature)
        }).keyBy(_._1)
                .timeWindow(Time.seconds(10))   //开一个10秒的窗口
                .reduce((x,y)=>(x._1,x._2.min(y._2)))
                .map(x => (x._1,x._2,System.currentTimeMillis()))

        //sin
        windowedStream.print("min temperature")

        env.execute("window test")
    }

}

class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
    val bound = 3*1000;//延迟20秒

    var maxTs = 0L

    override def getCurrentWatermark = {
        new Watermark(maxTs-bound)
    }

    override def extractTimestamp(t: SensorReading, l: Long) = {
        maxTs = maxTs.max(t.timestamp*1000)
        t.timestamp*1000
    }
}
