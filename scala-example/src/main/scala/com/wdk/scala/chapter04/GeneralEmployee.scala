package com.wdk.scala.chapter04

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:28
  * @Version: v1.0
  **/
class GeneralEmployee extends Employee {
    override var name: String = _
    override var monthAnnual: Double = _

    override def getAnnual(months: Int): Double = {
        monthAnnual*months
    }

    def work(): Unit ={
        println("死命干活.......")
    }
}
