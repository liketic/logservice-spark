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

import org.apache.spark.Partition

case class ShardPartition(rddId: Int,
                          partitionId: Int,
                          shardId: Int,
                          maxRecordsPerShard: Int,
                          project: String,
                          logstore: String,
                          accessKeyId: String,
                          accessKeySecret: String,
                          endpoint: String,
                          startCursor: String,
                          endCursor: String,
                          logGroupStep: Int = 100) extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + shardId

  override def equals(other: Any): Boolean = super.equals(other)

  override def index: Int = partitionId
}
