package com.wdk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Description
  *             自定义函数
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 11:36
  * @Since version 1.0.0
  */
object SparkSql_UDF {

    def main(args: Array[String]): Unit = {
        //创建配置对象sparkConf
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_transform")

        //创建SparkSQL执行对象
        val spark : SparkSession = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        //准备数据
        val userDf = spark.read.json("spark-example/in/user.json")

        //创建临时表
        userDf.createTempView("users")

        //注册UDF
        spark.udf.register("addName",addName(_:String))

        //使用UDF
        spark.sql("select addName(name) from users").show()

        spark.stop()
    }

    def addName(name:String):String={
        "Name:"+name
    }

}
