/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.LogHubConsumer;
import com.aliyun.openservices.loghub.client.LogHubHeartBeat;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.streaming.aliyun.logservice.utils.VersionInfoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientWorker implements Runnable {
    private static final Log LOG = LogFactory.getLog(ClientWorker.class);

    private final ILogHubProcessorFactory mLogHubProcessorFactory;
    private final LogHubConfig mLogHubConfig;
    private final LogHubHeartBeat mLogHubHeartBeat;
    private boolean mShutDown = false;
    private final Map<Integer, LogHubConsumer> mShardConsumer = new HashMap<Integer, LogHubConsumer>();
    private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
    private LogHubClientAdapter mLogHubClientAdapter;

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config)
            throws LogHubClientWorkerException {
        this.mLogHubProcessorFactory = factory;
        this.mLogHubConfig = config;

        String accessKeyId = config.getAccessId();
        String accessKeySecret = config.getAccessKey();
        String securityToken = config.getStsToken();
        String loghubEndpoint = config.getLogHubEndPoint();
        Client mClient = new Client(loghubEndpoint, accessKeyId, accessKeySecret);
        mClient.setUserAgent(VersionInfoUtils.getUserAgent() + "-" + config.getConsumerGroupName()
                + "/" + config.getConsumerName());
        this.mLogHubClientAdapter = new LogHubClientAdapter(
                loghubEndpoint, accessKeyId, accessKeySecret,
                securityToken, config.getProject(), config.getLogStore(),
                config.getConsumerGroupName(), config.getConsumerName(), false);
        try {
            this.mLogHubClientAdapter.CreateConsumerGroup(
                    (int) (config.getHeartBeatIntervalMillis() * 2L / 1000L),
                    config.isConsumeInOrder());
        } catch (LogException e) {
            if (!e.GetErrorCode().equalsIgnoreCase("ConsumerGroupAlreadyExist")) {
                throw new LogHubClientWorkerException("error occur when create " +
                        "consumer group, errorCode: " + e.GetErrorCode() +
                        ", errorMessage: " + e.GetErrorMessage());
            }
        }
        this.mLogHubHeartBeat = new LogHubHeartBeat(this.mLogHubClientAdapter,
                config.getHeartBeatIntervalMillis());
    }

    @Override
    public void run() {
        this.mLogHubHeartBeat.Start();
        ArrayList<Integer> heldShards = new ArrayList<Integer>();

        while (!this.mShutDown) {
            this.mLogHubHeartBeat.GetHeldShards(heldShards);

            for (int shard : heldShards) {
                LogHubConsumer consumer = this.getConsumer(shard);
                consumer.consume();
            }

            this.cleanConsumer(heldShards);

            try {
                Thread.sleep(this.mLogHubConfig.getDataFetchIntervalMillis());
            } catch (InterruptedException e) {
                // make compiler happy.
            }
        }
    }

    public void shutdown() {
        this.mShutDown = true;
        this.mLogHubHeartBeat.Stop();
    }

    private void cleanConsumer(ArrayList<Integer> ownedShard) {
        ArrayList<Integer> removeShards = new ArrayList<Integer>();
        Iterator it = this.mShardConsumer.entrySet().iterator();

        while (it.hasNext()) {
            Entry shard = (Entry) it.next();
            LogHubConsumer consumer = (LogHubConsumer) shard.getValue();
            if (!ownedShard.contains(shard.getKey())) {
                consumer.shutdown();
                LOG.warn("try to shut down a consumer shard:" + shard.getKey());
            }

            if (consumer.isShutdown()) {
                this.mLogHubHeartBeat.RemoveHeartShard((Integer) shard.getKey());
                removeShards.add((Integer) shard.getKey());
                LOG.warn("remove a consumer shard:" + shard.getKey());
            }
        }

        it = removeShards.iterator();

        while (it.hasNext()) {
            int shard1 = (Integer) it.next();
            this.mShardConsumer.remove(shard1);
        }

    }

    private LogHubConsumer getConsumer(int shardId) {
        LogHubConsumer consumer = this.mShardConsumer.get(shardId);
        if (consumer != null) {
            return consumer;
        } else {
            consumer = new LogHubConsumer(
                    this.mLogHubClientAdapter,
                    shardId,
                    this.mLogHubConfig.getConsumerName(),
                    this.mLogHubProcessorFactory.generatorProcessor(),
                    this.mExecutorService,
                    this.mLogHubConfig.getCursorPosition(),
                    this.mLogHubConfig.GetCursorStartTime(),
                    this.mLogHubConfig.getMaxFetchLogGroupSize());
            this.mShardConsumer.put(shardId, consumer);
            LOG.warn("create a consumer shard:" + shardId);
            return consumer;
        }
    }
}
