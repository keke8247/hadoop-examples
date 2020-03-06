package com.wdk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  *              使用sparksql 操作hive
  *              需要把hive-site.xml配置文件 放到resources下面
  *              声明sparkSession的时候 需要制定 enableHiveSupport
  * @Author:wang_dk
  * @Date:2020 /3/6 0006 20:58
  * @Version: v1.0
  **/
object SparkSql_hive {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_hive")

        val spark = SparkSession.builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate()

        spark.sql("select * from badou.orders limit 20").show()

    }
}
