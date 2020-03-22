/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.aliyun.logservice

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._


class ZkHelper(zkParams: Map[String, String],
               checkpointDir: String,
               project: String,
               logstore: String) extends Logging {

  private val zkDir = s"$checkpointDir/commit/$project/$logstore"

  @transient private var zkClient: ZkClient = _
  @transient private val latestOffsets: util.Map[Int, String] = new java.util.concurrent.ConcurrentHashMap[Int, String]()

  def initialize(): Unit = synchronized {
    if (zkClient != null) {
      // already initialized
      return
    }
    val zkConnect = zkParams.getOrElse("zookeeper.connect", "localhost:2181")
    val zkSessionTimeoutMs = zkParams.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
    val zkConnectionTimeoutMs =
      zkParams.getOrElse("zookeeper.connection.timeout.ms", zkSessionTimeoutMs.toString).toInt
    logInfo(s"zkDir = $zkDir")

    zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    zkClient.setZkSerializer(new ZkSerializer() {
      override def serialize(data: scala.Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        if (bytes == null) {
          return null
        }
        new String(bytes, StandardCharsets.UTF_8)
      }
    })
  }

  def markOffset(shard: Int, cursor: String): Unit = synchronized {
    latestOffsets.put(shard, cursor)
  }

  private def decideCursorToTs(cursor: String): Long = {
    val bytes = Base64.getDecoder.decode(cursor.getBytes(StandardCharsets.UTF_8))
    new String(bytes, StandardCharsets.UTF_8).toLong
  }

  def checkValidOffset(shard: Int, cursor: String): Boolean = {
    val prev = latestOffsets.get(shard)
    if (prev == null) {
      return true
    }
    val pts = decideCursorToTs(prev)
    val cts = decideCursorToTs(cursor)
    if (cts >= pts) {
      // maybe previous fetch returned nothing
      return true
    }
    logWarning(s"invalid cursor $cursor, prev $prev")
    false
  }

  def cleanupRDD(rddID: Int, shard: Int): Unit = {
    initialize()
    deleteIfExists(s"$zkDir/$rddID/$shard")
  }

  def mkdir(): Unit = {
    initialize()
    try {
      if (zkClient.exists(zkDir)) {
        zkClient.getChildren(zkDir).foreach(child => {
          zkClient.deleteRecursive(s"$zkDir/$child")
        })
      } else {
        zkClient.createPersistent(zkDir, true)
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Loghub direct api depends on zookeeper. Make sure " +
          "zookeeper is available.", e)
    }
  }

  def readOffset(shardId: Int): String = {
    initialize()
    zkClient.readData(s"$zkDir/$shardId.shard", true)
  }

  def readEndOffset(rddID: Int, shardId: Int): String = {
    initialize()
    // TODO Wait data exists
    val path = s"$zkDir/$shardId/$rddID"
    zkClient.readData(path, true)
  }

  def tryMarkEndOffset(rddID: Int, shardId: Int, cursor: String): Boolean = {
    initialize()
    val path = s"$zkDir/$shardId/$rddID"
    if (zkClient.exists(path)) {
      false
    } else {
      zkClient.createPersistent(path, true)
      zkClient.writeData(path, cursor)
      true
    }
  }

  private def writeData(path: String, data: String): Unit = {
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true)
    }
    zkClient.writeData(path, data)
  }

  def saveOffset(shard: Int, cursor: String): Unit = {
    initialize()
    val path = s"$zkDir/$shard.shard"
    logDebug(s"Save $cursor to $path")
    writeData(path, cursor)
  }

  def tryLock(shard: Int): Boolean = {
    initialize()
    val lockFile = s"$zkDir/$shard.lock"
    try {
      zkClient.createPersistent(lockFile, false)
      true
    } catch {
      case _: ZkNodeExistsException =>
        logWarning(s"$shard already locked")
        false
    }
  }

  private def deleteIfExists(path: String): Unit = {
    try {
      zkClient.delete(path)
    } catch {
      case _: ZkNoNodeException =>
      // ignore
    }
  }

  def unlock(shard: Int): Unit = {
    initialize()
    deleteIfExists(s"$zkDir/$shard.lock")
  }

  def close(): Unit = synchronized {
    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }
  }
}

object ZkHelper extends Logging {

  private case class CacheKey(zkParams: Map[String, String],
                              checkpointDir: String,
                              project: String,
                              logstore: String)

  private var cache: util.HashMap[CacheKey, ZkHelper] = _

  def getOrCreate(zkParams: Map[String, String],
                  checkpointDir: String,
                  project: String,
                  logstore: String): ZkHelper = synchronized {
    if (cache == null) {
      cache = new util.HashMap[CacheKey, ZkHelper]()
    }
    val k = CacheKey(zkParams, checkpointDir, project, logstore)
    var zkHelper = cache.get(k)
    if (zkHelper == null) {
      zkHelper = new ZkHelper(zkParams, checkpointDir, project, logstore)
      zkHelper.initialize()
      cache.put(k, zkHelper)
    }
    zkHelper
  }
}