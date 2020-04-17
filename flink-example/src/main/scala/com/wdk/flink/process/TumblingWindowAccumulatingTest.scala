package com.wdk.flink.process

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              测试数据集:
  *              第二次（或多次）触发的条件是watermark < end-of-window + allowedLateness时间内，这个窗口有late数据到达
                    key1,1487225040000
                    key1,1487225046000
                    key1,1487225044000
                    key1,1487225050000
                    key1,1487225055000
                    key1,1487225058000
                    key1,1487225045000
                    key1,1487225048000
                    key1,1487225058000
                    key1,1487225049000
                    key1,1487225063000
                    key1,1487225069000
                    key1,1487225071000
                    key1,1487225074000
                    key1,1487225083000
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 13:59
  * @Version: v1.0
  **/
object TumblingWindowAccumulatingTest {
    def  main(args : Array[String]) : Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment //获取流处理执行环境
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置Event Time作为时间属性


        val input = env.socketTextStream("master",7777) //socket接收数据

        val inputMap = input.map(f=> {
            val arr = f.split(",")
            val code = arr(0)
            val time = arr(1).toLong
            (code,time)
        })

        /**
          * 允许3秒的乱序
          */
        val watermarkDS = inputMap
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)]
                (Time.seconds(3)) {
            override def extractTimestamp(element: (String, Long)): Long = element._2
        })

        /**
          * 对于此窗口而言，允许5秒的迟到数据，即第一次触发是在watermark > end-of-window时
          * 第二次（或多次）触发的条件是watermark < end-of-window + allowedLateness时间内，这个窗口有late数据到达
          */
        val accumulatorWindow = watermarkDS
                .keyBy(_._1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .apply(new AccumulatingWindowFunction)
                .name("window accumulate test")

        accumulatorWindow.print()

        env.execute()

    }
}


class AccumulatingWindowFunction extends RichWindowFunction[(String, Long),( Long,String,String, Int),String,TimeWindow]{

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    var state: ValueState[Int] = _

    var count = 0

    override def open(config: Configuration): Unit = {
        state = getRuntimeContext.getState(new ValueStateDescriptor[Int]("AccumulatingWindow Test", classOf[Int], 0))
    }

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(Long,String,String, Int)]): Unit = {
        count = state.value() + input.size

        var values = new StringBuilder();

        input.foreach(item=>{
            values.append(item._2+"|")
        })

        state.update(count)

        // key,window start time, window end time, window size, system time, total size
        out.collect(window.getEnd, format.format(window.getEnd),values.toString(),count)
    }
}
