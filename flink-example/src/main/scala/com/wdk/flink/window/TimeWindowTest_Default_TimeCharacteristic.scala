package com.wdk.flink.window

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Description:
  *              时间窗口测试
  *              不指定env.setStreamTimeCharacteristic() 默认是按照ProcessTime
  *              flink收到第一条数据  开启一个10秒的时间窗口, 10秒后关闭第一个窗口 处理窗口数据.
  *              然后开启 下一个10秒的时间窗口.
  *                 min temperature> (sensor_2,30.992934,1583044160001)
  *                 ...中间没有数据进来 1583044170001这个时间窗口没有输出...
                    min temperature> (sensor_1,34.992934,1583044180001)
                    min temperature> (sensor_1,34.992934,1583044190001)
                    min temperature> (sensor_1,34.992934,1583044200002)

  * @Author:wang_dk
  * @Date:2020 /3/1 0001 14:07
  * @Version: v1.0
  **/
object TimeWindowTest_Default_TimeCharacteristic {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //Source
        val input = env.socketTextStream("master",7777).map(line => {
            val attr = line.split(",")
            SensorReading(attr(0).trim,attr(1).trim.toLong,attr(2).trim.toDouble)
        }).map( item => (item.id,item.temperature))
                .keyBy(_._1)
                .timeWindow(Time.seconds(10)) //开一个10秒的窗口  如果不指定Stream时间特性  默认是按照ProcessTime
                .reduce((x,y) => (x._1,x._2.min(y._2))) //统计窗口时间内 最小的温度值
                .map(x=>(x._1,x._2,System.currentTimeMillis()))

        //sin
        input.print("min temperature")

        env.execute("window test")
    }

}
