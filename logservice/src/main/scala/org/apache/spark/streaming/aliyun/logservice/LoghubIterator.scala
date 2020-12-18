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

import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.util.NextIterator

import scala.collection.JavaConversions._

class LoghubIterator(rddID: Int,
                     zkClient: ZkClientWrapper,
                     client: LoghubClientAgent,
                     part: ShardPartition,
                     context: TaskContext)
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var cursor = part.fromCursor
  private var endCursor: String = _
  private val shardId = part.shardId
  private val maxRecordsPerShard = part.maxRecordsPerShard
  private var buffer = new LinkedBlockingQueue[String](maxRecordsPerShard)
  private var hasNextBatch = true
  private var unlocked = false
  private var isFetchEndCursorCalled = false

  private val inputMetrics = context.taskMetrics.inputMetrics

  context.addTaskCompletionListener { _ => closeIfNeeded() }

  private def unlock(): Unit = {
    if (!unlocked) {
      zkClient.unlock(shardId)
      logDebug(s"unlock shard $shardId")
      unlocked = true
    }
  }

  override protected def getNext(): String = {
    if (hasRead < maxRecordsPerShard && hasNextBatch) {
      if (buffer.isEmpty) {
        fetchNextBatch()
      }
    }
    if (buffer.isEmpty) {
      finished = true
      if (zkClient.tryMarkEndOffset(rddID, shardId, cursor)) {
        zkClient.saveOffset(shardId, cursor)
      }
      unlock()
      null
    } else {
      hasRead += 1
      buffer.poll()
    }
  }

  override def close() {
    try {
      unlock()
      inputMetrics.incBytesRead(hasRead)
      buffer.clear()
      buffer = null
    } catch {
      case e: Exception => logWarning("Exception when close LoghubIterator.", e)
    }
  }

  private def fetchEndCursor(): Unit = {
    if (!isFetchEndCursorCalled) {
      // try to fetch end cursor of this shard in current RDD
      endCursor = zkClient.readEndOffset(rddID, shardId)
      isFetchEndCursorCalled = true
    }
  }

  def fetchNextBatch(): Unit = {
    fetchEndCursor()
    val r = client.BatchGetLog(part.project, part.logstore, shardId, part.batchSize, cursor, endCursor)
    if (r == null) {
      return
    }
    var count = 0
    r.GetLogGroups().foreach(group => {
      val fastLogGroup = group.GetFastLogGroup()
      val logCount = fastLogGroup.getLogsCount
      count += logCount
      for (i <- 0 until logCount) {
        val log = fastLogGroup.getLogs(i)
        val topic = fastLogGroup.getTopic
        val source = fastLogGroup.getSource
        val obj = new JSONObject()
        obj.put(__TIME__, log.getTime)
        obj.put(__TOPIC__, topic)
        obj.put(__SOURCE__, source)
        obj.put(__SHARD__, shardId)
        val fieldCount = log.getContentsCount
        for (j <- 0 until fieldCount) {
          val f = log.getContents(j)
          obj.put(f.getKey, f.getValue)
        }
        if (!part.ignoreTags) {
          for (i <- 0 until fastLogGroup.getLogTagsCount) {
            val tag = fastLogGroup.getLogTags(i)
            obj.put("__tag__:".concat(tag.getKey), tag.getValue)
          }
        }
        buffer.offer(obj.toJSONString)
      }
    })
    val nextCursor = r.GetNextCursor()
    if (log.isDebugEnabled) {
      logDebug(s"shardId: $shardId, currentCursor: $cursor, nextCursor: $nextCursor," +
        s" hasRead: $hasRead, count: $count," +
        s" get: $count, queue: ${buffer.size()}")
    }
    if (cursor.equals(nextCursor)) {
      logDebug(s"No data at cursor $cursor in shard $shardId")
      hasNextBatch = false
    } else {
      cursor = nextCursor
    }
  }
}