package com.wdk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Description
  *             使用sparksql
  *             使用sql 处理数据
  *             转换 RDD ==> DataFrame ==> DataSet ==> DataFrame ==> RDD
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/3/6 20:37
  * @Since version 1.0.0
  */
object SparkSql_transform {
    def main(args: Array[String]): Unit = {
        //创建配置对象sparkConf
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql_transform")

        //创建SparkSQL执行对象
        val spark : SparkSession = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        //创建一个RDD
        val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",22),(3,"wagnwu",24),(4,"maliu",25)))

        //在进行rdd df ds 转换之前 需要引入 sparkSession的隐式转换
        import spark.implicits._    //这里的spark 不是包名 是 前面声明的SparkSession对象.切记

        //rdd ==> df rdd转Df 其实就是给Rdd加上结构
        val df = rdd.toDF("id","name","age")

        //df写入到csv文件 带表头
        val saveoptions = Map(
            "header"->"true",
            "delimiter"->"\t",
            "path"->"spark-example/out/df.csv")
        df.write.format("csv").options(saveoptions).mode(SaveMode.Overwrite).save()


        //df ==> ds df转ds 在结构的基础上加上类型
        val ds = df.as[User]

        //ds ==> df ds转df 把类型去掉 只保留结构
        val df1 = ds.toDF()

        //df ==> rdd df转rdd 把结构也去掉 形成 类型为Row 的RDD[Row]
        val rdd1 = df1.rdd
        rdd1.foreach(row=>{
            println("id:"+row.getInt(0)+" name:"+row.getString(1)+" age:"+row.getInt(2))  //从Row里面取数据 是根据索引取的
        })
        df1.show()

        //rdd ====> DS rdd直接转成DataSet  需要给Rdd先加上机构和类型(转成样例类) 然后再转成DS
        val userRDD = rdd.map(x=>(User(x._1,x._2,x._3)))
        val userDs = userRDD.toDS()

        //ds =====> rdd
        val userRDD1 = userDs.rdd
        userRDD1.foreach(println)


        //把df ds 转换成表 然后用sql操作表
        df.createTempView("user_tempView")  //创建临时表
        spark.sql("select * from user_tempView").show()

        df.createOrReplaceTempView("user_replaceTempView")  //如果当前有 user_replaceTempView 的话则重新创建并覆盖
        df.createGlobalTempView("user_globalTempView")  //创建全局表() 可以跨session访问, 临时表是只能在当前session内访问.

        //跨session访问临时表是不可以的
//        spark.newSession().sql("select * from user_tempView").show() //org.apache.spark.sql.AnalysisException: Table or view not found:

        spark.newSession().sql("select * from global_temp.user_globalTempView").show() //访问全局表  表名前面要加上 global_temp 前缀

        //释放资源
        spark.stop()
    }
}

case class User(id:Int,name:String,age:Int)
