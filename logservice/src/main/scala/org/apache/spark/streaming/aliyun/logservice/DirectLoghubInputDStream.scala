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
import com.aliyun.openservices.log.common.ConsumerGroup
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DirectLoghubInputDStream(_ssc: StreamingContext,
                               project: String,
                               logstore: String,
                               consumerGroup: String,
                               accessKeyId: String,
                               accessKeySecret: String,
                               endpoint: String,
                               zkParams: Map[String, String],
                               mode: LogHubCursorPosition,
                               cursorStartTime: Long = -1L)
  extends InputDStream[String](_ssc) with Logging with CanCommitOffsets {
  @transient private var zkHelper: ZkHelper = _
  @transient private var loghubClient: LoghubClientAgent = _
  @transient private var checkpointMgr: CheckpointManager = _

  private val initialRate = context.sparkContext.getConf.getLong(
    "spark.streaming.backpressure.initialRate", 0)
  private val maxRate = _ssc.sparkContext.getConf.getInt("spark.streaming.loghub.maxRatePerShard", 10000)
  // Cache 5 minutes by default
  private val shardCacheTimeout = _ssc.sparkContext.getConf.getInt("spark.streaming.loghub.shardCacheTimeout", 300 * 1000)
  // commit checkpoint interval
  private val checkpointCommitInterval = _ssc.sparkContext.getConf.getInt("spark.streaming.loghub.checkpointCommitInterval", 60 * 1000)

  private var checkpointDir: String = _
  private val noMoreDataShardCache = new mutable.HashMap[Int, String]()
  private val readOnlyShardEndCursorCache = new mutable.HashMap[Int, String]()
  private val currentOffsets = new mutable.HashMap[Int, String]()

  override def start(): Unit = {
    var zkCheckpointDir = ssc.checkpointDir
    if (StringUtils.isBlank(zkCheckpointDir)) {
      zkCheckpointDir = s"/$consumerGroup"
      logInfo(s"Checkpoint dir was not specified, using consumer group $consumerGroup as " +
        s"checkpoint dir")
    }
    checkpointDir = new Path(zkCheckpointDir).toUri.getPath
    initialize()
  }

  private def initialize(): Unit = this.synchronized {
    if (loghubClient == null) {
      loghubClient = LoghubClient.getOrCreate(endpoint,
        accessKeyId,
        accessKeySecret,
        consumerGroup)
    }
    if (zkHelper == null) {
      zkHelper = ZkHelper.getOrCreate(zkParams, checkpointDir, project, logstore)
      zkHelper.mkdir()
      val checkpoints = createConsumerGroupOrGetCheckpoint()
      loghubClient.listShardWithCache(project, logstore, shardCacheTimeout).GetShards().foreach(r => {
        val shardId = r.GetShardId()
        val offset = findCheckpointOrCursorForShard(shardId, checkpoints)
        zkHelper.saveOffset(shardId, offset)
      })
    }
  }

  def setClient(client: LoghubClientAgent): Unit = {
    loghubClient = client
  }

  override def stop(): Unit = this.synchronized {
    if (zkHelper != null) {
      zkHelper.close()
      zkHelper = null
    }
    if (checkpointMgr != null) {
      checkpointMgr.close()
      checkpointMgr = null
    }
  }

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectLoghubRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  protected[streaming] def maxRecordsPerShard(shardCount: Int): Int = {
    val estimatedRateLimit = rateController.map { x => {
      val lr = x.getLatestRate()
      if (lr > 0) lr else initialRate
    }
    }
    val ratePerShard = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        val backpressureRate = rate * 1.0 / shardCount
        Math.min(backpressureRate, maxRate)
      case None => maxRate
    }
    (ratePerShard * (ssc.graph.batchDuration.milliseconds / 1000)).toInt
  }

  private def endCursorForReadOnlyShard(shardId: Int): String = {
    var endCursor = readOnlyShardEndCursorCache.getOrElse(shardId, null)
    if (endCursor == null) {
      endCursor = loghubClient.GetCursor(project, logstore, shardId, CursorMode.END).GetCursor()
      readOnlyShardEndCursorCache.put(shardId, endCursor)
    }
    endCursor
  }

  def checkCursorNotRollback(shard: Int, cursor: String): Boolean = {
    val previous = currentOffsets.getOrElse(shard, null)
    if (previous != null && ShardUtils.checkCursorLessThan(cursor, previous)) {
      logError(s"Previous cursor $previous " +
        s"has been rollback to $cursor")
      return false
    }
    true
  }

  override def compute(validTime: Time): Option[RDD[String]] = {
    val beginTime = System.currentTimeMillis()
    initialize()
    val shardOffsets = new ArrayBuffer[InternalOffsetRange]()
    // Ten durations or 1 second
    val lockTimeoutSec = Math.max(ssc.graph.batchDuration.milliseconds / 100, 1)
    loghubClient.listShardWithCache(project, logstore, shardCacheTimeout).GetShards().foreach(shard => {
      val shardId = shard.GetShardId()
      if (noMoreDataShardCache.contains(shardId)) {
        logInfo(s"There is no data to consume from shard $shardId.")
      } else if (zkHelper.tryLock(shardId, lockTimeoutSec)) {
        var start = zkHelper.readOffset(shardId)
        if (start == null || start.isEmpty) {
          // this is the first fetching of this shard
          start = fetchCheckpointOrInitialCursor(shardId)
        }
        var end: String = null
        var skip = false
        if (shard.getStatus.equalsIgnoreCase("readonly")) {
          end = endCursorForReadOnlyShard(shardId)
          if (start.equals(end)) {
            logInfo(s"Skip empty $shardId end cursor $start")
            noMoreDataShardCache.put(shardId, end)
            skip = true
          }
        }
        if (!skip && checkCursorNotRollback(shardId, start)) {
          shardOffsets.add(InternalOffsetRange(shardId, start, end))
          logInfo(s"Shard $shardId start from $start")
          currentOffsets.put(shardId, start)
        } else {
          zkHelper.unlock(shardId)
        }
      }
    })
    val rdd = new LoghubRDD(
      ssc.sc,
      project,
      logstore,
      consumerGroup,
      accessKeyId,
      accessKeySecret,
      endpoint,
      zkParams,
      shardOffsets.toArray,
      maxRecordsPerShard(shardOffsets.size),
      checkpointDir).setName(s"LoghubRDD-${validTime.toString()}")
    val description = shardOffsets.map { p =>
      val offset = "offset: [ %1$-30s to %2$-30s ]".format(p.fromCursor, p.untilCursor)
      s"shardId: ${p.shardId}\t $offset"
    }.mkString("\n")
    val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    if (storageLevel != StorageLevel.NONE) {
      // If storageLevel is not `StorageLevel.NONE`, we need to persist rdd before `count()` to
      // to count the number of records to avoid refetching data from loghub.
      rdd.persist(storageLevel)
      logDebug(s"Persisting RDD ${rdd.id} for time $validTime to $storageLevel")
    }
    val timeBeforeCount = System.currentTimeMillis()
    val inputInfo = StreamInputInfo(id, rdd.count(), metadata)
    val now = System.currentTimeMillis()
    logInfo(s"Call compute cost ${now - beginTime}, count cost ${now - timeBeforeCount}")
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    Some(rdd)
  }

  /**
   * Commit the offsets to LogService at a future time. Threadsafe.
   * Users should call this method at end of each output operation.
   */
  override def commitAsync(offsetRanges: Array[OffsetRange]): Unit = {
    if (checkpointMgr == null) {
      initialize()
      checkpointMgr = new CheckpointManager(project, logstore, consumerGroup,
        loghubClient,
        checkpointCommitInterval,
        zkHelper)
      checkpointMgr.start()
    }
    if (offsetRanges != null) {
      // commit begin cursor of previous batch
      import scala.collection.JavaConverters._
      checkpointMgr.commit(offsetRanges.toList.asJava)
    }
  }

  private[streaming] override def name: String = s"Loghub direct stream [$id]"

  def createConsumerGroupOrGetCheckpoint(): mutable.Map[Int, String] = {
    try {
      loghubClient.CreateConsumerGroup(project,
        logstore,
        new ConsumerGroup(consumerGroup, 10, true))
      null
    } catch {
      case e: LogException =>
        if (e.GetErrorCode.equalsIgnoreCase("ConsumerGroupAlreadyExist")) {
          fetchAllCheckpoints()
        } else {
          throw new LogHubClientWorkerException("Cannot create consumer group, " +
            "errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage())
        }
    }
  }

  private def fetchAllCheckpoints(): mutable.Map[Int, String] = {
    val checkpoints = new mutable.HashMap[Int, String]()
    try {
      val fetched = loghubClient.ListCheckpoints(project, logstore, consumerGroup)
      if (fetched != null) {
        fetched.getCheckPoints.map(x => (x.getShard, x.getCheckPoint))
          .filter(tp => tp._2 != null && !tp._2.isEmpty)
          .foreach(r => checkpoints.put(r._1, r._2))
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot fetch checkpoint from remote server", e)
    }
    checkpoints
  }

  def findCheckpointOrCursorForShard(shardId: Int,
                                     checkpoints: mutable.Map[Int, String]): String = {
    if (checkpoints != null) {
      val checkpoint = checkpoints.getOrElse(shardId, null)
      if (checkpoint != null && !checkpoint.isEmpty) {
        logInfo(s"Shard $shardId will start from checkpoint $checkpoint")
        return checkpoint
      }
    }
    fetchInitialCursor(shardId)
  }

  private def fetchCheckpointOrInitialCursor(shardId: Int): String = {
    var cursor = loghubClient.fetchCheckpoint(project, logstore, consumerGroup, shardId)
    if (cursor == null || cursor.isEmpty) {
      cursor = fetchCheckpointOrInitialCursor(shardId)
    }
    cursor
  }

  private def fetchInitialCursor(shardId: Int): String = {
    val cursor = mode match {
      case LogHubCursorPosition.END_CURSOR =>
        loghubClient.GetCursor(project, logstore, shardId, CursorMode.END)
      case LogHubCursorPosition.BEGIN_CURSOR =>
        loghubClient.GetCursor(project, logstore, shardId, CursorMode.BEGIN)
      case LogHubCursorPosition.SPECIAL_TIMER_CURSOR =>
        loghubClient.GetCursor(project, logstore, shardId, cursorStartTime)
    }
    val initialCursor = cursor.GetCursor()
    logInfo(s"Start reading shard $shardId from $initialCursor")
    initialCursor
  }

  private class DirectLoghubInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    override def update(time: Time): Unit = {}

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {}
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectLoghubRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }

  override def finalize(): Unit = {
    super.finalize()
    stop()
  }
}
