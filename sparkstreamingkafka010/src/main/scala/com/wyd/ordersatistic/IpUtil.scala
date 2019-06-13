package com.wyd.ordersatistic

object IpUtil {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L

    for(i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(Long,Long,String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high){
      val middle = (low+high) / 2
      if(ip >= lines(middle)._1 && (ip <= lines(middle)._2)){
        return  middle
      }
      if(ip < lines(middle)._1){
        high = middle -1
      } else {
        low = middle + 1
      }
    }
    -1
  }

}
