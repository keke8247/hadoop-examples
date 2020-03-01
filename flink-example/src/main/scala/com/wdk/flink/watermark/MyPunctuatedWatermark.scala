package com.wdk.flink.watermark

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Description:
  *              非周期的watermark
  *              有监控到数据 决定watermark的生成
  * @Author:wang_dk
  * @Date:2020 /3/1 0001 11:59
  * @Version: v1.0
  **/
class MyPunctuatedWatermark extends AssignerWithPunctuatedWatermarks[SensorReading]{
    val bond : Long = 60*1000 //延时1分钟


    override def checkAndGetNextWatermark(t: SensorReading, l: Long) = {
        if(t.id == "sensor_1"){ //SensorReading id为 sensor_1的数据进来 生成watermark
            new Watermark(l-bond)
        }else{
            null
        }
    }

    override def extractTimestamp(t: SensorReading, l: Long) = {
        t.timestamp
    }
}
