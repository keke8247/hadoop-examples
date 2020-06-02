package com.wdk.scala.chapter01

/**
  * @Description
  *             某人有100000元,没经过一次路口 需要交费 规则如下:
  *             1.当现金>50000时,每次缴5%
  *             2.当现金<=50000时,每次缴1000
  *             需求: 计算该人可以经过多少次路口.
  * @Author rdkj
  * @CreatTime 2020/5/29 10:31
  * @Since version 1.0.0
  */
object Demo04 {
    def main(args: Array[String]): Unit = {
        var total = 100000

        var nums = 0
        while(total >1000){
            if(total > 50000){
                total -= (total*0.05).toInt
            }else{
                total -= 1000
            }
            nums += 1
        }
        printf("当前还剩下:%d元,一共经过了%d次路口",total,nums)
    }
}
