package com.wdk.flink.process

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description
  *             侧输出流 可以把一条输入流 分流输出
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/2 9:54
  * @Since version 1.0.0
  */
object SideOutputTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("master",7777).map(line =>{
            val attr = line.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
            override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
        })

        //温度低于32度 低温报警
        val dataStream = inputStream.process(new FreezingAlert())

        dataStream.print("正常温度:")

        //这里的 new OutPutTag("low-temperature") 要和往侧输出流写入时候贴的标签 一致
        dataStream.getSideOutput(new OutputTag[String]("low-temperature")).print("低温报警:")

        env.execute("process function test")
    }

}

class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{

    //定义一个 outputTag 标签  标识侧输出流
    lazy val freezing = new OutputTag[String]("low-temperature")
    override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]) = {
        if(value.temperature < 32){
            context.output(freezing,"温度太低了......"+value.toString)
        }else{
            collector.collect(value)
        }
    }
}
