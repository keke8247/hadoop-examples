package com.wdk.flink.transformer

import com.wdk.flink.domain.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * @Description
  *             Flink 一些转换算子 使用测试
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/28 11:08
  * @Since version 1.0.0
  */
object TransformerTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置全局并行度为1

        val inputStream = env.readTextFile("F:\\myGitHub\\hadoop\\hadoop-examples\\flink-example\\main\\resources\\sensor.txt")

//        1:转换算子
        //map 对数据流里面的每个元素做处理 返回一个新的DataStream
        val mapDs = inputStream.map(line => {
            val item = line.split(",")
            SensorReading(item(0).trim,item(1).trim.toLong,item(2).trim.toDouble)
        })
//        mapDs.print("map")

        //flatMap 会把输入list里面的元素 打平到一个结果List里面 返回一个新的DataStream
        val flatMapDs = inputStream.flatMap(line => line.split(","))
//        flatMapDs.print("flatMap")

        //filter 根据过滤条件 筛选输入流 返回一个新的DataStream
        val filterDs = mapDs.filter(item=> item.temperature>30)
//        filterDs.print("filter")

        //keyBy 根据某一个键分组 返回KeyedStream
        val keyByKs:KeyedStream[SensorReading,String] = mapDs.keyBy(_.id)
//        keyByKs.print("keyBy")

        //sum 根据某一个属性求和  sum是针对KeyedStream 进行操作的
        val sumDs = keyByKs.sum(2)
//        sumDs.print("sum")

        //输出当前最新温度+10,而时间戳是上一个数据的时间戳+1 使用reduce 聚合算子.(x,y)=> ... x为上一次聚合的结果,y为当前的输入
        val reduce = keyByKs.reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature+10))
//        reduce.print("reduce")


        //2.分流算子 split和select 通过split操作 可以把一个DataSteam内部分成多个不同的Stream可以贴上不同的标志 组装成SplitStream返回  大于32 为high 低于32为low
        val splitStream = mapDs.split(item=>{
            if(item.temperature>32)
                Seq("high")
            else
                Seq("low")
        })

        //使用select函数 对split分出的多个splitStream进行分拣 形成多个DataStream
        val high = splitStream.select("high")
        val low = splitStream.select("low")

        //可以输入多个参数 选择出符合的流
        val all = splitStream.select("high","low")

//        high.print("high")
//        low.print("low")
//        all.print("all")

        //3.Connect合并流 CoMap Connect 只能一次连接两个DataStream 类型可以不同
        // 可以连接两个不同类型的流 把high转换一下  然后 connect 上 low 会返回一个ConnectedStreams[流1的类型,流2的类型]
        val connectedStreams = high.map(item=>(item.id,item.temperature)).connect(low)
        //通过对 ConnectedStreams 使用map算子 可以把ConnectedSreams 转换成DataStrem
        val coMap = connectedStreams.map(
            high=>(high._1,high._2),
            low=>(low.id,low.timestamp,low.temperature)
        )
//        coMap.print("coMap")

        //4:Union 一次可以连接多个流 但是 流的类型必须一致
        val unionDS = high.union(low).union(all)
        unionDS.print("unionDs")

        env.execute("transform test")
    }
}

