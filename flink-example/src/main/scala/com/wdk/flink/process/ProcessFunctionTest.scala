package com.wdk.flink.process

import com.wdk.flink.domain.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description:
  *              10秒内 温度连续上升 触发报警信息......
  * @Author:wang_dk
  * @Date:2020 /3/1 0001 16:44
  * @Version: v1.0
  **/
object ProcessFunctionTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("master",7777).map(line =>{
            val attr = line.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
            override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
        })

        val keyedStream = inputStream.keyBy(_.id)
                .process(new MyKeyedProcessFunction())

        inputStream.print("temperature>>>")

        keyedStream.print("waring:")

        env.execute("process function test")
    }
}

class MyKeyedProcessFunction() extends KeyedProcessFunction[String,SensorReading,String]{
    //定义一个状态 保存上一条数据温度信息  这里必须使用lazy修饰 因为env还没有执行 环境还没有初始化完成 不然会报 The runtime context has not been initialized
    lazy val preTemp : ValueState[Double]= getRuntimeContext.getState(new ValueStateDescriptor[Double]("preTemp",classOf[Double]))
    //定义一个状态，用来保存定时器的时间戳
    lazy val preTime : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("preTime",classOf[Long]));

    override def processElement(sensorReading: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]) = {
        //获取上一条温度
        val temp = preTemp.value();
        //获取当前温度
        val currentTemp = sensorReading.temperature
        //把当前温度更新到最新状态
        preTemp.update(currentTemp)

        //获取上一次处理时间
        val time = preTime.value()

        if(currentTemp > temp && time == 0){    //如果温度上升 并且没有注册过定时器
            //获取当前时间
            val timerTs = ctx.timerService().currentProcessingTime() +10000L

            ctx.timerService().registerProcessingTimeTimer(timerTs) //注册一个10秒的定时器

            preTime.update(timerTs)
        }else if(currentTemp < temp || time == 0){ //如果温度下降 或者是第一条数据  清除定时器状态
            ctx.timerService().deleteProcessingTimeTimer(time)
            preTime.clear()
        }
    }

    //重写onTimer方法 如果定时器触发  回调onTimer方法
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]) = {
        out.collect(ctx.getCurrentKey + "温度持续上升.....请留意!")
        preTime.clear()
    }
}





