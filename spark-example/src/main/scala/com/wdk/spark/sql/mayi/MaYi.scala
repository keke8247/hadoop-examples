package com.wdk.spark.sql.mayi

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Description:
  *              蚂蚁金服面试题
  * @Author:wang_dk
  * @Date:2020-06-28 21:09
  * @Version: v1.0
  **/
object MaYi {

    /*
    *   背景说明：
        以下表记录了用户每天的蚂蚁森林低碳生活领取的记录流水。
        table_name：user_low_carbon
        user_id data_dt  low_carbon
        用户     日期      减少碳排放（g）

        蚂蚁森林植物换购表，用于记录申领环保植物所需要减少的碳排放量
        table_name:  plant_carbon
        plant_id plant_name low_carbon
        植物编号	植物名	换购植物所需要的碳
    * */

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("蚂蚁金服面试题...")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        //设置需要使用的库
        sparkSession.sql("use wdk")

        method1(sparkSession)


    }

    //题目1
    /**
      * ----题目
        1.蚂蚁森林植物申领统计
        问题：假设2017年1月1日开始记录低碳数据（user_low_carbon），
        假设2017年10月1日之前满足申领条件的用户都申领了一颗p004-胡杨，
        剩余的能量全部用来领取“p002-沙柳” 。
        统计在10月1日累计申领“p002-沙柳” 排名前10的用户信息；以及他比后一名多领了几颗沙柳。
        得到的统计结果如下表样式：
        user_id  plant_count less_count(比后一名多领了几颗沙柳)
        u_101    1000         100
        u_088    900          400
        u_103    500          …
      *
      * */
    def method1(sparkSession: SparkSession): Unit ={

        //step1 查询胡杨需要的 碳
        val p004Carbon: DataFrame = sparkSession.sql("select low_carbon as lowCarBon from plant_carbon where plant_id = 'p004'")

        val p002Carbon: DataFrame = sparkSession.sql("select low_carbon as p2CarBon from plant_carbon where plant_id = 'p002'")


        //step2 查询2017年10月1日前 用户领取的碳数量
        val totalCarbon: DataFrame = sparkSession.sql("select user_id,sum(low_carbon) totalCarbon from user_low_carbon where data_dt <= ('2017/10/1') group by user_id")

        import sparkSession.implicits._

        //2017年10月1前领取的碳数量
        totalCarbon.createOrReplaceTempView("total_carbon")

        p004Carbon.createTempView("p004_carbon")
        p002Carbon.createTempView("p002_carbon")

        //设置支持 笛卡尔积 操作 .默认不支持
        sparkSession.sql("set spark.sql.crossJoin.enabled=true")

        val plantCountDF: DataFrame = sparkSession.sql("select user_id,floor((totalCarbon-lowCarBon)/p2CarBon)  as plant_count  from  total_carbon, p004_carbon,p002_carbon")

        plantCountDF.createTempView("plan_count_tmp")

        sparkSession.sql("select user_id,plant_count,plant_count-(lead(plant_count,1,0) over(order by plant_count desc)) as p2 from plan_count_tmp").show()

    }
}

