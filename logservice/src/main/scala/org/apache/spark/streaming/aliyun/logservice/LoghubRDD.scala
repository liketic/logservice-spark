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
                val offsets: Array[InternalOffsetRange],
                val maxRecordsPerShard: Int,
                val checkpointDir: String)
  extends RDD[String](sc, Nil) with Logging with HasOffsetRanges {

  @transient var client: LoghubClientAgent = _
  @transient var zkHelper: ZkHelper = _

  private def initialize(): Unit = {
    zkHelper = ZkHelper.getOrCreate(zkParams, checkpointDir, project, logstore)
    client = LoghubClient.getOrCreate(endpoint, accessKeyId, accessKeySecret, consumerGroup)
  }

  override def count(): Long = {
    // We cannot get count with cursor ranges
    super.count()
  }

  def offsetRanges: Array[OffsetRange] = {
    // Hack RDD id here
    offsets.map(r => OffsetRange(id, r.shardId, r.fromCursor, r.untilCursor))
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    initialize()
    val partition = split.asInstanceOf[ShardPartition]
    val iter = new LoghubIterator(id, zkHelper, client, partition, context)
    new InterruptibleIterator[String](context, iter)
  }

  override protected def getPartitions: Array[Partition] = {
    val batchSize = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
    offsetRanges.zipWithIndex.map { case (p, idx) =>
      ShardPartition(id, idx,
        p.shardId,
        maxRecordsPerShard,
        project,
        logstore,
        accessKeyId,
        accessKeySecret,
        endpoint,
        p.fromCursor,
        batchSize).asInstanceOf[Partition]
    }
  }
}