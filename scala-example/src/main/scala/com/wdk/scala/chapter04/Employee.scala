package com.wdk.scala.chapter04

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:26
  * @Version: v1.0
  **/
abstract class Employee {

    var name : String
    var monthAnnual : Double

    def getAnnual(months:Int): Double
}
