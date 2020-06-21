package com.wdk.scala.chapter04

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:29
  * @Version: v1.0
  **/
class ManagerEmployee extends Employee {
    override var name: String = _
    override var monthAnnual: Double = _
    var bonus : Double = _

    override def getAnnual(months: Int): Double = {
        bonus + monthAnnual*months
    }

    def manage(): Unit ={
        println(name + "开始指挥干活.....")
    }
}
