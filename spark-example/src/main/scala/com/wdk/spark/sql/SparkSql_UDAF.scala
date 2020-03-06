package com.wdk.spark.sql

import com.wdk.spark.sql.SparkSql_UDF.addName
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * @Description
  *             自定义聚合函数
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 11:48
  * @Since version 1.0.0
  */
object SparkSql_UDAF {
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

        //创建函数
        val ageAvg = new MyAgeAvgFunction

        //注册函数
        spark.udf.register("ageAvg",ageAvg)

        //使用函数
        spark.sql("select ageAvg(age) as avgAge from users").show()

        spark.stop()
    }
}

// 自定义 计算年龄平均值聚合函数  需要继承 UserDefinedAggregateFunction
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
    //函数入参结构类型
    override def inputSchema: StructType = {
        new StructType().add("age",LongType)
    }

    //函数计算需要用到的结构类型  计算平均值 需要 sum/count
    override def bufferSchema: StructType = {
        new StructType().add("sum",LongType).add("count",LongType)
    }

    //返回数据类型  Double
    override def dataType: DataType = DoubleType

    //是否稳定函数
    override def deterministic: Boolean = true

    //初始化 sum count
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        //这里只能根据下标取属性  buffer(0)为sum / buffer(1)为count
        buffer(0) = 0L
        buffer(1) = 0L
    }


    //数据进来 更新计算结果
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
    }

    //合并不同节点的计算结果
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }


    //计算
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}
