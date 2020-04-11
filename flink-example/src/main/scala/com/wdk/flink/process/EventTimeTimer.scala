package com.wdk.flink.process

import com.wdk.flink.domain.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /4/11 0011 10:08
  * @Version: v1.0
  **/
object EventTimeTimer {

    def main(args: Array[String]): Unit = {
        //定义flink的执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        //定义输入流 并转换成sensorReading
        val inputStream = env.socketTextStream("master",7777).map(item=>{
            val attr = item.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
            override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
        })


        // 处理数据流 温度持续上升输出报警信息
        val processStream = inputStream.keyBy(_.id)
                .process(new TempWarningAlert())

        inputStream.print("input data>>>")

        processStream.print("warning data>>>")

        env.execute("温度监控......")
    }

}

//温度持续上升ProcessFunction
class TempWarningAlert() extends KeyedProcessFunction[String,SensorReading,String]{
    lazy val preTempState : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("preTempState",classOf[Double]))

    lazy val currentTimerState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimerSate",classOf[Long]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]) = {
        //取出上一次的温度
        val preTemp = preTempState.value();

        //把当前温度更新到状态
        preTempState.update(value.temperature)

        //当前定时器时间戳
        val currentTimerTs = currentTimerState.value()

        //如果温度持续上升 并且当前没有定时器 注册定时器
        if(value.temperature > preTemp && currentTimerTs == 0){
            //当前watermark 之后3秒触发
            val timerTs = ctx.timerService().currentWatermark()+3000L

            ctx.timerService().registerEventTimeTimer(timerTs)

            currentTimerState.update(timerTs)
        }else if(value.temperature < preTemp || preTemp == 0.00){ //如果温度下降 或者第一次进来  删除定时器信息
            ctx.timerService().deleteEventTimeTimer(currentTimerTs)

            //清空状态
            currentTimerState.clear()
        }
    }

    //定时器触发报警信息
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]) = {
        out.collect(ctx.getCurrentKey+" 温度持续上升!!!")
        currentTimerState.clear()
    }
}
