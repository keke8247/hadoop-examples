package com.wdk.flink.watermark

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @Description:
  *              自定义周期性watermark
  *              系统周期行的调用 获取时间戳
  * @Author:wang_dk
  * @Date:2020 /3/1 0001 11:52
  * @Version: v1.0
  **/
class MyPeriodicWatermarks extends AssignerWithPeriodicWatermarks[SensorReading]{

    val bond : Long = 60*1000   //延时1分钟
    var maxTs : Long = Long.MinValue


    //获取 watermark
    override def getCurrentWatermark : Watermark= {
        new Watermark(maxTs-bond)   //监控到最大时间戳 -bond
    }


    //提取时间戳
    override def extractTimestamp(t: SensorReading, l: Long) = {
        maxTs = maxTs.max(t.timestamp)  //比较时间戳 和 当前maxTs的大小
        t.timestamp*1000    //提取时间戳
    }
}
