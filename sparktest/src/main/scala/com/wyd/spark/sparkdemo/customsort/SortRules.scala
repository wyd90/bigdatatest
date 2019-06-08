package com.wyd.spark.sparkdemo.customsort

object SortRules {

  implicit val rule = new Ordering[(String, Int, Int)] {
    override def compare(x: (String, Int, Int), y: (String, Int, Int)): Int = {
      if(y._3 - x._3 == 0){
        x._2 - y._2
      } else {
        y._3 - x._3
      }
    }
  }

}
