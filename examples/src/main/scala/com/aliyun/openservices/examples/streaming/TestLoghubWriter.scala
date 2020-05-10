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
package com.aliyun.openservices.examples.streaming

import com.aliyun.openservices.aliyun.log.producer.{Callback, Result}
import com.aliyun.openservices.log.common.LogItem
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.aliyun.logservice.writer._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.util.parsing.json.JSON

object TestLoghubWriter {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: TestLoghubWriter <sls project> <sls logstore> <sls target logstore> <sls endpoint>
          |         <access key id> <access key secret>
                """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val targetLogstore = args(2)
    val loghubGroupName = args(3)
    val endpoint = args(4)
    val accessKeyId = args(5)
    val accessKeySecret = args(6)
    val batchInterval = Milliseconds(5 * 1000)
    val zkAddress = "localhost:2181"

    val conf = new SparkConf().setAppName("Test write data to Loghub")
      .setMaster("local[1]")
      .set("spark.streaming.loghub.maxRatePerShard", "10")
      .set("spark.loghub.batchGet.step", "1")
      .set("spark.loghub.ignoreTags", "true")
    val zkParas = Map("zookeeper.connect" -> zkAddress)
    val ssc = new StreamingContext(conf, batchInterval)

    val loghubStream = LoghubUtils.createDirectStream(
      ssc,
      loghubProject,
      logStore,
      loghubGroupName,
      accessKeyId,
      accessKeySecret,
      endpoint,
      zkParas,
      LogHubCursorPosition.BEGIN_CURSOR)

    val producerConfig = Map(
      "sls.project" -> loghubProject,
      "sls.logstore" -> targetLogstore,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "sls.endpoint" -> endpoint,
      "sls.ioThreadCount" -> "2"
    )

    val lines = loghubStream.map(x => x)

    val myConversionFunc = { input: String => Integer.parseInt(input) }
    // Convert numberic field to int instead of double
    JSON.globalNumberParser = myConversionFunc

    def transformFunc(jsonStr: String): (String, LogItem) = {
      val result = JSON.parseFull(jsonStr)
      result match {
        case Some(map: Map[String, Any]) =>
          val item = new LogItem()
          var hashkey: String = null
          map.foreach(it => {
            it._2 match {
              case str: String =>
                item.PushBack(it._1, str)
              case _: Double =>
                println(s"${it._1} is double: ${it._2}")
              case other: Int =>
                if (it._1.equals("__shard__")) {
                  // shard as hash key
                  hashkey = it._2.toString
                } else if (it._1.equals("__time__")) {
                  // copy log time
                  item.SetTime(other)
                }
            }
          })
          (hashkey, item)
        case None =>
          // ignore
          (null, null)
      }
    }

    val callback = new Callback with Serializable {
      override def onCompletion(result: Result): Unit = {
        // scalastyle:off
        println(s"Send result ${result.isSuccessful}")
        // scalastyle:on
      }
    }

    lines.writeToLoghubWithHashKey(
      producerConfig,
      "topic",
      "streaming",
      transformFunc,
      Option.apply(callback))

    ssc.checkpoint("/tmp/spark/streaming") // set checkpoint directory
    ssc.start()
    ssc.awaitTermination()
  }
}
