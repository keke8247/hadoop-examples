package com.wdk.flink.wc

import org.apache.flink.api.scala._

/**
  * @Description
  *             使用flink,批处理做 workcount
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/27 10:06
  * @Since version 1.0.0
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        //创建flink 批处理执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment

        val inputFile = "F:\\myGitHub\\hadoop\\hadoop-examples\\flink-example\\main\\resources\\test.txt"

        val inputDataSet = env.readTextFile(inputFile)

        val wordCount = inputDataSet
            .flatMap(_.split(" "))
            .map((_,1))
            .groupBy(0) //根据元组中 第一个元素 分组
            .sum(1)     //根据元组中 第二个元素 求和

        wordCount.print()
    }
}
