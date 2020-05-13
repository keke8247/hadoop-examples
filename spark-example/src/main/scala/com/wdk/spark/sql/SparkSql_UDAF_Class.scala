package com.wdk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @Description
  *             强类型的UDAF
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 13:54
  * @Since version 1.0.0
  */
object SparkSql_UDAF_Class {
    def main(args: Array[String]): Unit = {
        //创建配置对象sparkConf
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_transform")

        //创建SparkSQL执行对象
        val spark : SparkSession = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        //准备数据
        val userDf = spark.read.json("spark-example/in/user.json")

        userDf.show()

        //创建聚合函数对象
        val udaf = new MyAgeAvgClassFunction

        //由于聚合函数接收的参数是UserBean对象类型, 不能传单个列 所以 将聚合函数转换为要查询的列
        val ageAvg: TypedColumn[UserBean, Double] = udaf.toColumn.name("ageAvg")

        //由于是强类型  所以需要 DataSet
        val userDs: Dataset[UserBean] = userDf.as[UserBean]

        //应用函数 使用DSL风格  直接用select 方法
        userDs.select(ageAvg).show()

        spark.stop()
    }
}

//自定义聚合函数 强类型
// 继承Aggregator  UserBean传入的参数类型 ,AvgBuffer 计算时用到的类型, Double计算结果返回类型
class MyAgeAvgClassFunction() extends Aggregator[UserBean,AvgBuffer,Double] {

    //初始化 计算 AvgBuffer
    override def zero = {
        AvgBuffer(0L,0)
    }

    //数据进来 累加age count
    override def reduce(b: AvgBuffer, a: UserBean) = {
        b.sum = b.sum + a.age
        b.count = b.count + 1
        b
    }

    //合并不同节点的处理结果
    override def merge(b1: AvgBuffer, b2: AvgBuffer) = {
        b1.sum = b1.sum + b2.sum
        b1.count = b1.count + b2.count
        b1
    }

    //执行最终的计算
    override def finish(reduction: AvgBuffer) = {
        reduction.sum.toDouble / reduction.count
    }

    //转码
    override def bufferEncoder = Encoders.product

    //转码
    override def outputEncoder = Encoders.scalaDouble
}

case class UserBean(id:BigInt,name:String,age:BigInt)

case class AvgBuffer(var sum : BigInt,var count : Int)


