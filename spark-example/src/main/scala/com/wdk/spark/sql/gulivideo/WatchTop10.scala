package com.wdk.spark.sql.gulivideo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Description:
  *              谷粒影音
  * @Author:wang_dk
  * @Date:2020-06-28 20:52
  * @Version: v1.0
  **/
object GuliVideoAnalysis {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GuliVideo")

        //构建sparkSession
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import sparkSession.implicits._

        sparkSession.sql("use glyy")

        //视频观看次数 top10
        sparkSession.sql("select *,rank() over(order by views desc) as rank from gulivideo_ori  limit 10").show()

    }
}
