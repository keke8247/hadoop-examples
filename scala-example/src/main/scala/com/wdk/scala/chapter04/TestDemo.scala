package com.wdk.scala.chapter04

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-21 10:31
  * @Version: v1.0
  **/
object TestDemo {

    def main(args: Array[String]): Unit = {
        println(showEmpAnnal())

        val employee = new GeneralEmployee
        employee.name = "lishi"
        employee.monthAnnual = 3000

        testWork(employee)

        val employee1 = new ManagerEmployee
        employee1.name = "hahah"
        testWork(employee1)

    }

    def showEmpAnnal(): Double ={
        val employee: GeneralEmployee = new GeneralEmployee
        employee.name = "zhangsan"
        employee.monthAnnual = 10000.00
        employee.getAnnual(12)
    }

    def testWork(emp : Employee): Unit ={
        if(emp.isInstanceOf[GeneralEmployee]){
            val employee: GeneralEmployee = emp.asInstanceOf[GeneralEmployee]
            employee.work()
        }else if(emp.isInstanceOf[ManagerEmployee]){
            val manage: ManagerEmployee = emp.asInstanceOf[ManagerEmployee]
            manage.manage()
        }
    }

}
