package org.apache.spark.streaming.aliyun.logservice

import java.util

object LoghubClient {

  private case class CacheKey(accessKeyId: String, accessKeySecret: String, endpoint: String)

  private var cache: util.HashMap[CacheKey, LoghubClientAgent] = _

  def getOrCreate(accessKeyId: String,
                  accessKeySecret: String,
                  endpoint: String): LoghubClientAgent = synchronized {
    if (cache == null) {
      cache = new util.HashMap[CacheKey, LoghubClientAgent]()
    }
    val k = CacheKey(accessKeyId, accessKeySecret, endpoint)
    var client = cache.get(k)
    if (client == null) {
      client = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
      cache.put(k, client)
    }
    client
  }
}
