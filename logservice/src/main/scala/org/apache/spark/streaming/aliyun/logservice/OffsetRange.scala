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


/**
 * Loghub offset range.
 *
 * @param shardId     Loghub shard id.
 * @param fromCursor  The start cursor of range.
 * @param untilCursor The end cursor of range which may be null.
 */
case class OffsetRange(rddID: Int, logstore: String, shardId: Int, fromCursor: String, untilCursor: String)

case class InternalOffsetRange(logstore: String, shardId: Int, fromCursor: String, untilCursor: String)

trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

trait CanCommitOffsets {
  /**
   * Queue up offset ranges for commit to Loghub at a future time.  Threadsafe.
   * This is only needed if you intend to store offsets in Loghub, instead of your own store.
   */
  def commitAsync(offsets: Array[OffsetRange]): Unit
}
