package com.wdk.scala.chapter01

/**
  * @Description
  *             打印九九乘法表
  * @Author rdkj
  * @CreatTime 2020/5/29 10:04
  * @Since version 1.0.0
  */
object Demo01 {
    def main(args: Array[String]): Unit = {
        for (i <- 1 to 9){
            for(j <- 1 to i){
                printf("%d * %d = %d  ", j,i,i*j)
            }
            println()
        }
    }
}
