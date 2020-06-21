package com.wdk.scala.chapter03

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:15
  * @Version: v1.0
  **/
class Computer {

    var cpu:String = _
    var memory : String = _
    var disk : String = _

    def getDetails(): Unit ={
        println("CPU:" + cpu)
        println("memory:" + memory)
        println("disk:" + disk)
    }
}
