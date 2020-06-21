package com.wdk.scala.chapter03

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:17
  * @Version: v1.0
  **/
object TestDemo {
    def main(args: Array[String]): Unit = {
        val computer = new Computer()
        computer.cpu = "Intel"
        computer.memory = "32GB"
        computer.disk = "1TB"

        computer.getDetails()


        val pc = new PC
        pc.brand = "联想"
        pc.cpu = "AMD"
        pc.memory = "32GB"
        pc.disk = "1TB"
        println("CPU:"+pc.cpu +"\t memory:"+pc.memory + "\t disk:"+pc.disk+"\t brand:"+ pc.brand)

    }
}
