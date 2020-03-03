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

import com.aliyun.openservices.log.common.Consts.CursorMode

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class LoghubRDD(@transient sc: SparkContext,
                val project: String,
                val logstore: String,
                val consumerGroup: String,
                val accessKeyId: String,
                val accessKeySecret: String,
                val endpoint: String,
                val zkParams: Map[String, String],
                val offsetRanges: Array[OffsetRange],
                val maxRecordsPerShard: Int,
                val checkpointDir: String) extends RDD[String](sc, Nil) with Logging with HasOffsetRanges {
  @transient var client: LoghubClientAgent = _
  @transient var zkHelper: ZkHelper = _
  private val enablePreciseCount: Boolean =
    sc.getConf.getBoolean("spark.streaming.loghub.count.precise.enable", defaultValue = true)

  private def initialize(): Unit = {
    client = LoghubRDD.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
    zkHelper = LoghubRDD.getZkHelper(zkParams, checkpointDir, project, logstore)
  }

  override def count(): Long = {
    if (enablePreciseCount) {
      super.count()
    } else {
      try {
        offsetRanges.map(shard => {
          val from = client.GetCursorTime(project, logstore, shard.shardId, shard.fromCursor)
            .GetCursorTime()
          val endCursor =
            client.GetCursor(project, logstore, shard.shardId, CursorMode.END).GetCursor()
          val to = client.GetCursorTime(project, logstore, shard.shardId, endCursor)
            .GetCursorTime()
          val res = client.GetHistograms(project, logstore, from, to, "", "*")
          if (!res.IsCompleted()) {
            logWarning(s"Failed to get complete count for [$project]-[$logstore]-" +
              s"[${shard.shardId}] from ${shard.fromCursor} to ${endCursor}, " +
              s"use ${res.GetTotalCount()} instead. " +
              s"This warning does not introduce any job failure, but may affect some information " +
              s"about this batch.")
          }
          (res.GetTotalCount() * 1.0D) / offsetRanges.length
        }).sum.toLong
      } catch {
        case e: Exception =>
          logWarning(s"Failed to get statistics of rows in [$project]-[$logstore], use 0L " +
            s"instead. This warning does not introduce any job failure, but may affect some " +
            s"information about this batch.", e)
          0L
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    try {
      val loghubIterator = new LoghubIterator(zkHelper, client, project, logstore,
        consumerGroup, shardPartition.shardId, shardPartition.startCursor,
        shardPartition.maxRecordsPerShard, context, shardPartition.logGroupStep)
      new InterruptibleIterator[String](context, loghubIterator)
    } catch {
      case _: Exception =>
        Iterator.empty.asInstanceOf[Iterator[String]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val logGroupStep = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
    offsetRanges.zipWithIndex.map { case (p, idx) =>
      new ShardPartition(id, idx, p.shardId, maxRecordsPerShard, project, logstore, consumerGroup,
        accessKeyId, accessKeySecret, endpoint, p.fromCursor, logGroupStep)
        .asInstanceOf[Partition]
    }
  }

  private class ShardPartition(rddId: Int,
                               partitionId: Int,
                               val shardId: Int,
                               val maxRecordsPerShard: Int,
                               val project: String,
                               val logstore: String,
                               val consumerGroup: String,
                               val accessKeyId: String,
                               val accessKeySecret: String,
                               val endpoint: String,
                               val startCursor: String,
                               val logGroupStep: Int = 100) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId

    override def equals(other: Any): Boolean = super.equals(other)

    override def index: Int = partitionId
  }

}

object LoghubRDD extends Logging {
  private var zkHelper: ZkHelper = _
  private var loghubClient: LoghubClientAgent = _

  def getOrCreateLoghubClient(accessKeyId: String,
                              accessKeySecret: String,
                              endpoint: String): LoghubClientAgent = {
    if (loghubClient == null) {
      loghubClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
    }
    loghubClient
  }

  def getZkHelper(zkParams: Map[String, String],
                  checkpointDir: String,
                  project: String,
                  logstore: String): ZkHelper = {
    if (zkHelper == null) {
      zkHelper = new ZkHelper(zkParams, checkpointDir, project, logstore)
      zkHelper.initialize()
    }
    zkHelper
  }

  override def finalize(): Unit = {
    super.finalize()
    try {
      if (zkHelper != null) {
        zkHelper.close()
        zkHelper = null
      }
    } catch {
      case e: Exception => logWarning("Exception when close zkClient.", e)
    }
  }
}
