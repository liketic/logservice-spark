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

import com.aliyun.openservices.log.Client
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class LoghubInputDStream(
    @transient _ssc: StreamingContext,
    logServiceProject: String,
    logStoreName: String,
    consumerGroupName: String,
    loghubInstanceNameBase: String,
    var loghubEndpoint: String,
    var accessKeyId: String,
    var accessKeySecret: String,
    storageLevel: StorageLevel,
    cursorPosition: LogHubCursorPosition,
    mLoghubCursorStartTime: Int,
    forceSpecial: Boolean)
  extends ReceiverInputDStream[Array[Byte]](_ssc) {
  private val consumeInOrder =
    _ssc.sc.getConf.getBoolean("spark.logservice.fetch.inOrder", true)
  private  val heartBeatIntervalMillis =
    _ssc.sc.getConf.getLong("spark.logservice.heartbeat.interval.millis", 30000L)
  private val dataFetchIntervalMillis =
    _ssc.sc.getConf.getLong("spark.logservice.fetch.interval.millis", 200L)
  private val batchInterval = _ssc.graph.batchDuration.milliseconds

  override def getReceiver(): Receiver[Array[Byte]] =
    new LoghubReceiver(
      consumeInOrder,
      heartBeatIntervalMillis,
      dataFetchIntervalMillis,
      batchInterval,
      logServiceProject,
      logStoreName,
      consumerGroupName,
      loghubInstanceNameBase,
      loghubEndpoint,
      accessKeyId,
      accessKeySecret,
      storageLevel,
      cursorPosition,
      mLoghubCursorStartTime)

  def this(@transient _ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubInstanceNameBase: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel) = {
    this(_ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      loghubInstanceNameBase, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel, LogHubCursorPosition.END_CURSOR, -1, false)
  }
}
