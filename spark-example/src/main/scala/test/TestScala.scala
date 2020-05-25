package test

/**
  * @Description
  * @Author rdkj
  * @CreatTime 2020/5/20 9:39
  * @Since version 1.0.0
  */
object TestScala {
    def main(args: Array[String]): Unit = {
        val myMap = Map("k1"->"v1");
        val val1 = myMap.get("k1")
        val val2 = myMap.get("k2")

        println(val1.getOrElse("null"))
        println(val2.getOrElse("null"))
    }

}
