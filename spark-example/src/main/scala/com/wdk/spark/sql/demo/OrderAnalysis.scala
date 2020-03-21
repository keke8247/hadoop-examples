package com.wdk.spark.sql.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, sql}

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/21 0021 11:00
  * @Version: v1.0
  **/
object OrderAnalysis {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderAnalysis")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        //加载数据
        val tbDateDS = spark.sparkContext.textFile("spark-example/in/tbDate.txt").map(item=>{
            val attr = item.split(",")
            TbDate(attr(0).trim,attr(1).trim.toInt,attr(2).trim.toInt,attr(3).trim.toInt,attr(4).trim.toInt,attr(5).trim.toInt,attr(6).trim.toInt,attr(7).trim.toInt,attr(8).trim.toInt,attr(9).trim.toInt)
        }).toDS()

        tbDateDS.show()

        val tbStockDS = spark.sparkContext.textFile("spark-example/in/tbStock.txt").map(item=>{
            val attr = item.split(",")
            TbStock(attr(0).trim,attr(1).trim,attr(2).trim)
        }).toDS()

        tbStockDS.show()

        val tbStockDetailDS = spark.sparkContext.textFile("spark-example/in/tbStockDetail.txt").map(item=>{
            val attr = item.split(",")
            TbStockDetail(attr(0).trim,attr(1).trim.toInt,attr(2).trim,attr(3).trim.toInt,attr(4).trim.toDouble,attr(5).trim.toDouble)
        }).toDS()

        tbStockDetailDS.show()


        //1.统计所有订单中每年的销售单数、销售总额
        tbStockDS.createOrReplaceTempView("tbStock")
        tbDateDS.createOrReplaceTempView("tbDate")
        tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

        spark.sql("select a.ordernumber,sum(b.amount) from tbStock a join tbStockDetail b on a.ordernumber = b.ordernumber group by a.ordernumber").show


    }

}

//TbData
case class TbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int)

//TBStock
case class TbStock (ordernumber:String,locationid:String,dateid:String);

//TBStockDetail
case class TbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double)