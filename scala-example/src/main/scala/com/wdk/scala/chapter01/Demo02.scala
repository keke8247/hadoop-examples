package com.wdk.scala.chapter01

import util.control.Breaks._

/**
  * @Description
  *             100以内的数求和. 求出当 和 第一次大于20的当前数
  * @Author rdkj
  * @CreatTime 2020/5/29 10:11
  * @Since version 1.0.0
  */
object Demo02 {
    def main(args: Array[String]): Unit = {
        method1
        println("________________________________")
        method2()
    }

    /*
    * 使用break() 跳出循环.
    * 1.需要把 循环体使用 breakable{}语句块包括起来
    * 2.需要 导入包 import util.control.Breaks._
    * */
    def method1(): Unit ={
        breakable{
            var sum = 0;
            for (i <- 1 to 100){
                sum += i
                if(sum > 20){
                    println("和大于20了,当前i="+i)
                    break()
                }
            }
        }
    }

    /**
      * 使用循环守卫的方式.满足需求. 做一个标记为flag
      * */
    def method2(): Unit ={
        var sum = 0;
        var flag = true
        for(i <- 1 to 100 if flag == true){
            sum += i

            if(sum >20){
                println("使用循环守卫方式,中断循环. 和大于20了,当前i="+i)
                flag = false;
            }
        }
    }

}
