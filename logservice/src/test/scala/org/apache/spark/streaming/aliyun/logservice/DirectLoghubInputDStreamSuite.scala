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

import java.util

import com.aliyun.openservices.log.common.Consts
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition

import scala.collection.mutable
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.aliyun.logservice.LoghubTestUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class DirectLoghubInputDStreamSuite extends SparkFunSuite {

  private val testUtils = new LoghubTestUtils
  private var sc: StreamingContext = _
  private val zkParas = Map("zookeeper.connect" -> "localhost:2181")
  private val conf = new SparkConf().setAppName("Test Direct SLS Loghub")
    .setMaster("local")
  private var logstores: util.Collection[String] = _

  test("test checkpoint empty will be filtered") {
    val stream = new DirectLoghubInputDStream(
      sc,
      testUtils.logProject,
      logstores,
      "consumergroup",
      testUtils.accessKeyId,
      testUtils.accessKeySecret,
      testUtils.endpoint,
      zkParas,
      LogHubCursorPosition.BEGIN_CURSOR
    )
    val client = new LoghubClientAgent(testUtils.endpoint,
      testUtils.accessKeyId,
      testUtils.accessKeySecret)
    stream.setClient(client)
    val ckpt = new mutable.HashMap[Int, String]()
    ckpt.put(0, "not-empty-cursor")
    ckpt.put(1, "")
    val logstore = logstores.iterator().next()
    val ckpt1 = stream.findCheckpointOrCursorForShard(logstore, 0, ckpt)
    assert(ckpt1 == "not-empty-cursor")
    val ckpt2 = stream.findCheckpointOrCursorForShard(logstore, 1, ckpt)
    val beginCursor = client.GetCursor(testUtils.logProject,
      logstore, 1, Consts.CursorMode.BEGIN).GetCursor()
    assert(ckpt2 == beginCursor)
  }

  test("test create consumer group") {
    val cg = "consumerGroup-1"
    val stream = new DirectLoghubInputDStream(
      sc,
      testUtils.logProject,
      logstores,
      cg,
      testUtils.accessKeyId,
      testUtils.accessKeySecret,
      testUtils.endpoint,
      zkParas,
      LogHubCursorPosition.BEGIN_CURSOR
    )
    val client = new LoghubClientAgent(testUtils.endpoint,
      testUtils.accessKeyId,
      testUtils.accessKeySecret)
    stream.setClient(client)
    val logstore = logstores.iterator().next()

    var checkpoints = stream.createConsumerGroupOrGetCheckpoint(logstore)
    assert(checkpoints.isEmpty)
    Thread.sleep(60000)
    testUtils.client.UpdateCheckPoint(testUtils.logProject,
      logstore,
      cg,
      0,
      "MTU3NTUzMDgyMDAzOTA5Mjk0MQ==")
    testUtils.client.UpdateCheckPoint(testUtils.logProject,
      logstore,
      cg,
      1,
      "MTU3NTUzMDgyMDA0MDk4NDQzOQ==")
    val ckpts = stream.createConsumerGroupOrGetCheckpoint(logstore)
    assert(ckpts.size == 2)
    assert(ckpts.getOrElse(0, "").equals("MTU3NTUzMDgyMDAzOTA5Mjk0MQ=="))
    assert(ckpts.getOrElse(1, "").equals("MTU3NTUzMDgyMDA0MDk4NDQzOQ=="))
  }

  protected override def beforeAll(): Unit = {
    val logstore = testUtils.newLogStore()
    testUtils.createLogStore(logstore)
    logstores = util.Arrays.asList(logstore)
    Thread.sleep(60000)
    val batchInterval = Milliseconds(5 * 1000)
    sc = new StreamingContext(conf, batchInterval)
  }

  protected override def afterAll(): Unit = {
    val it = logstores.iterator
    while (it.hasNext) {
      testUtils.deleteLogStore(it.next())
    }
  }
}
