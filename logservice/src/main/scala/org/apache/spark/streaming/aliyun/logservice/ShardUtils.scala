package org.apache.spark.streaming.aliyun.logservice

import java.nio.charset.StandardCharsets
import java.util.Base64

object ShardUtils {

  def decodeCursor(cursor: String): Long = {
    val bytes = Base64.getDecoder.decode(cursor.getBytes(StandardCharsets.UTF_8))
    new String(bytes, StandardCharsets.UTF_8).toLong
  }

  def checkCursorLessThan(lhs: String, rhs: String): Boolean = {
    val next = decodeCursor(lhs)
    val prev = decodeCursor(rhs)
    next < prev
  }

  def main(args: Array[String]): Unit = {
    println(checkCursorLessThan("MTYwMjc2MjA0NjI4MTAzMzIwOA==", "MTYwMjc2MjA0NjI4MTAzMzIwOA=="))
  }
}
