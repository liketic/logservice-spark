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
import com.aliyun.openservices.log.response.BatchGetLogResponse
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.util.NextIterator

import scala.collection.JavaConversions._

class LoghubIterator(zkHelper: ZkHelper,
                     client: LoghubClientAgent,
                     project: String,
                     logStore: String,
                     consumerGroup: String,
                     shardId: Int,
                     startCursor: String,
                     count: Int,
                     context: TaskContext,
                     logGroupStep: Int = 100)
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var cursor: String = startCursor
  private var logData = new LinkedBlockingQueue[String](1000 * logGroupStep)
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
    if (hasRead < count && shardEndNotReached) {
      if (logData.isEmpty) {
        fetchNextBatch()
      }
    }
    if (logData.isEmpty) {
      finished = true
      zkHelper.saveOffset(shardId, cursor)
      unlock()
      logDebug(s"unlock shard $shardId")
      null
    } else {
      hasRead += 1
      logData.poll()
    }
  }

  override def close() {
    try {
      unlock()
      inputMetrics.incBytesRead(hasRead)
      logData.clear()
      logData = null
    } catch {
      case e: Exception => logWarning("Exception when close LoghubIterator.", e)
    }
  }

  def fetchNextBatch(): Unit = {
    val response: BatchGetLogResponse =
      client.BatchGetLog(project, logStore, shardId, logGroupStep, cursor)
    var count = 0
    response.GetLogGroups().foreach(group => {
      val fastLogGroup = group.GetFastLogGroup()
      val logCount = fastLogGroup.getLogsCount
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
        count += 1
        logData.offer(obj.toJSONString)
      }
    })
    val nextCursor = response.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $cursor, nextCursor: $nextCursor," +
      s" hasRead: $hasRead, count: $count," +
      s" get: $count, queue: ${logData.size()}")
    if (cursor.equals(nextCursor)) {
      logDebug(s"No data at cursor $cursor in shard $shardId")
      shardEndNotReached = false
    } else {
      cursor = nextCursor
    }
  }
}