package com.wdk.flink.wc

import org.apache.flink.streaming.api.scala._

/**
  * @Description
  *             Flink 流式处理 workcount
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/27 10:27
  * @Since version 1.0.0
  */
object StreamWordCount {

    def main(args: Array[String]): Unit = {
        //创建Flink 流式处理 执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //创建数据流
        val intputStream = env.socketTextStream("master",7777)

        val wordCountDataStream = intputStream
            .flatMap(_.split(" "))
            .filter(_.nonEmpty)
            .map((_,1))
            .keyBy(0)   //把元组中第一个元素作为key
            .sum(1)     //根据元组中第二个元素求和

        wordCountDataStream
            .print()
            .setParallelism(2)  //设置并行度

        env.execute("stream word count job start......")
    }

}
