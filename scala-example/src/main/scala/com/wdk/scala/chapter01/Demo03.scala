package com.wdk.scala.chapter01

import scala.io.StdIn
import util.control.Breaks._

/**
  * @Description
  *             实现登录验证.有三次机会,如果用户名为"张无忌",密码为"888" 提示登录成功.否则提示还有几次机会,使用For循环
  * @Author rdkj
  * @CreatTime 2020/5/29 10:20
  * @Since version 1.0.0
  */
object Demo03 {
    def main(args: Array[String]): Unit = {

        breakable{
            for(i <- 1 to 3){
                println("请输入用户名:")
                val name = StdIn.readLine()

                println("用户名:"+name)

                println("请输入密码:")
                val pwd = StdIn.readLine("")

                println("密码:"+pwd)

                if(name.equals("张无忌") && "888".equals(pwd)){
                    println("登录成功......")
                    break()
                }else{
                    printf("用户名或密码输入失败.请重新输入.还有%d次机会\n",3-i)
                }
            }
        }
    }
}
