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
import java.util.Base64
import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.util.NextIterator

import scala.collection.JavaConversions._

class LoghubIterator(zkHelper: ZkHelper,
                     client: LoghubClientAgent,
                     project: String,
                     logstore: String,
                     part: ShardPartition,
                     context: TaskContext)
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var cursor = part.startCursor
  private var endCursor = part.endCursor
  private val shardId = part.shardId
  private var isFetchEndCursorCalled: Boolean = false
  private val maxRecordsPerShard = part.maxRecordsPerShard
  private val batchSize = part.logGroupStep
  private var buffer = new LinkedBlockingQueue[String](maxRecordsPerShard)
  private var shardEndNotReached: Boolean = true
  private var unlocked: Boolean = false
  private val inputMetrics = context.taskMetrics.inputMetrics

  context.addTaskCompletionListener { _ => closeIfNeeded() }

  private def unlock(): Unit = {
    if (!unlocked) {
      zkHelper.unlock(shardId)
      unlocked = true
    }
  }

  override protected def getNext(): String = {
    if (hasRead < maxRecordsPerShard && shardEndNotReached) {
      if (buffer.isEmpty) {
        fetchNextBatch()
      }
    }
    if (buffer.isEmpty) {
      finished = true
      zkHelper.saveOffset(shardId, cursor)
      unlock()
      logDebug(s"unlock shard $shardId")
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

  private def decideCursorToTs(cursor: String): Long = {
    val bytes = Base64.getDecoder.decode(cursor.getBytes(StandardCharsets.UTF_8))
    new String(bytes, StandardCharsets.UTF_8).toLong
  }

  private def fetchEndCursor(): Unit = {
    if (!isFetchEndCursorCalled) {
      if (endCursor != null) {
        // For read only shard, the cursor range will be provided
        isFetchEndCursorCalled = true
        return
      }
      endCursor = zkHelper.readOffset(shardId)
      if (cursor.equals(endCursor)) {
        // end cursor was not updated which means we're the first fetching.
        endCursor = null
      } else {
        val beginTs = decideCursorToTs(cursor)
        val endTs = decideCursorToTs(endCursor)
        if (beginTs >= endTs) {
          // End cursor maybe a earlier cursor, this should
          // never happen in normal case.
          endCursor = null
        }
      }
      isFetchEndCursorCalled = true
    }
  }

  def fetchNextBatch(): Unit = {
    fetchEndCursor()
    val response = client.BatchGetLog(project, logstore, shardId, batchSize, cursor, endCursor)
    if (response == null) {
      return
    }
    var count = 0
    response.GetLogGroups().foreach(group => {
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
        val fieldCount = log.getContentsCount
        for (j <- 0 until fieldCount) {
          val f = log.getContents(j)
          obj.put(f.getKey, f.getValue)
        }
        for (i <- 0 until fastLogGroup.getLogTagsCount) {
          val tag = fastLogGroup.getLogTags(i)
          obj.put("__tag__:".concat(tag.getKey), tag.getValue)
        }
        buffer.offer(obj.toJSONString)
      }
    })
    val nextCursor = response.GetNextCursor()
    if (log.isDebugEnabled) {
      logDebug(s"shardId: $shardId, currentCursor: $cursor, nextCursor: $nextCursor," +
        s" hasRead: $hasRead, count: $count," +
        s" get: $count, queue: ${buffer.size()}")
    }
    if (cursor.equals(nextCursor)) {
      logDebug(s"No data at cursor $cursor in shard $shardId")
      shardEndNotReached = false
    } else {
      cursor = nextCursor
    }
  }
}