package com.wdk.flink.source

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /2/27 0027 21:04
  * @Version: v1.0
  **/
class MyFlinkSource extends RichParallelSourceFunction[String]{
    var flag = true

    override def cancel() = {
        flag = false
    }

    override def run(sourceContext: SourceFunction.SourceContext[String]) = {
        while (flag){
            1.to(10).foreach(i=>{
                sourceContext.collect("MyFlinkSources_"+i+"...........")
            })
            TimeUnit.SECONDS.sleep(5)
        }
    }
}
