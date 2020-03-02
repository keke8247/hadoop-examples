package com.wdk.flink.state

import com.wdk.flink.domain.SensorReading
import com.wdk.flink.process.MyKeyedProcessFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description:
  *              Flink 状态编程
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
        })

        inputStream.print("temperature>>>")

        //使用process
//        val keyedStream = inputStream.keyBy(_.id)
//                .process(new TempChangeAlert(10.0))
//
//        keyedStream.print("waring:")

        //使用 flatMap
//        val flatMap = inputStream.keyBy(_.id)
//            .flatMap(new FlatMapTempChangeAlert(10.0))
//
//        flatMap.print("waring:")

        //使用FlatMapWithState
        val flatMapAndState = inputStream.keyBy(_.id)
            .flatMapWithState[(String,Double,Double),Double]({
            case (input,None) => (List.empty,Some(input.temperature))
            case (input,lastTemp) => {
                val diff = (input.temperature - lastTemp.get).abs
                if (diff > 10.0){
                    (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
                }else{
                    (List.empty,Some(input.temperature))
                }
            }
        })
        flatMapAndState.print("waring:")

        env.execute("process function test")
    }
}

//使用FlatMap 实现 前后两次温度差值 超过阈值 报警
class FlatMapTempChangeAlert(threshold : Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
    var lastTempState : ValueState[Double] = null;

    //在open中 初始化状态信息
    override def open(parameters: Configuration) = {
        lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
    }

    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]) = {
        //获取前一次温度
        val lastTemp = lastTempState.value();

        //计算差值 绝对值
        val diff = (in.temperature - lastTemp).abs

        if(diff > threshold){
            collector.collect((in.id,lastTemp,in.temperature))
        }
        lastTempState.update(in.temperature)
    }
}

// 前后两次温度差值 超过阈值 报警
class TempChangeAlert(threshold : Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)]{

    //定义一个状态 保存前一次温度数据
    lazy val lastTempState : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]));

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String,Double,Double)]#Context, out: Collector[(String,Double,Double)]) = {
        //获取前一次温度
        val lastTemp = lastTempState.value();

        //计算差值 绝对值
        val diff = (value.temperature - lastTemp).abs

        if(diff > threshold){
            out.collect((value.id,lastTemp,value.temperature))
        }
        lastTempState.update(value.temperature)
    }
}




