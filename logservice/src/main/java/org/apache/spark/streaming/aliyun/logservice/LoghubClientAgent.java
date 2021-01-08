/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.CreateConsumerGroupResponse;
import com.aliyun.openservices.log.response.GetCursorResponse;
import com.aliyun.openservices.log.response.GetCursorTimeResponse;
import com.aliyun.openservices.log.response.GetHistogramsResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class LoghubClientAgent {
    private static final Log LOG = LogFactory.getLog(LoghubClientAgent.class);

    private Client client;

    private static class CacheItem<T> {
        T data;
        long expireTime;
    }

    private Map<String, CacheItem<ListShardResponse>> shardCache;

    public LoghubClientAgent(String endpoint, String accessId, String accessKey) {
        this.client = new Client(endpoint, accessId, accessKey);
    }

    public void setUserAgent(String userAgent) {
        client.setUserAgent(userAgent);
    }

    private ListShardResponse getShardsFromCache(String logstore) {
        CacheItem<ListShardResponse> cacheItem = shardCache.get(logstore);
        if (cacheItem == null) {
            return null;
        }
        if (cacheItem.expireTime < System.currentTimeMillis()) {
            return null;
        }
        return cacheItem.data;
    }

    public ListShardResponse listShardWithCache(String project, String logstore, long timeout) throws Exception {
        if (shardCache == null) {
            this.shardCache = new HashMap<>();
        } else {
            ListShardResponse response = getShardsFromCache(logstore);
            if (response != null) {
                LOG.info("Shard cache hit, project " + project + ", logstore " + logstore);
                return response;
            }
        }
        ListShardResponse response = ListShard(project, logstore);
        CacheItem<ListShardResponse> item = new CacheItem<>();
        item.data = response;
        item.expireTime = System.currentTimeMillis() + timeout;
        shardCache.put(logstore, item);
        return response;
    }

    public ListShardResponse ListShard(String logProject, String logstore)
            throws Exception {
        return RetryUtil.call(() -> client.ListShard(logProject, logstore));
    }

    public GetCursorResponse GetCursor(String project, String logStream, int shardId, Consts.CursorMode mode)
            throws Exception {
        return RetryUtil.call(() -> client.GetCursor(project, logStream, shardId, mode));
    }

    public boolean safeUpdateCheckpoint(String project, String logstore, String consumerGroup,
                                        int shard, String checkpoint) {
        try {
            client.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
            return true;
        } catch (LogException ex) {
            LOG.warn("Unable to commit checkpoint: " + ex.GetErrorMessage());
        }
        return false;
    }

    public GetCursorResponse GetCursor(String project, String logstore, int shardId, long fromTime) throws Exception {
        return RetryUtil.call(() -> client.GetCursor(project, logstore, shardId, fromTime));
    }

    public CreateConsumerGroupResponse CreateConsumerGroup(String project, String logstore, ConsumerGroup consumerGroup)
            throws Exception {
        return RetryUtil.call(() -> client.CreateConsumerGroup(project, logstore, consumerGroup));
    }

    public String fetchCheckpoint(String project, String logstore, String consumerGroup, int shard)
            throws Exception {
        return RetryUtil.call(() -> {
            ConsumerGroupCheckPointResponse response = client.GetCheckPoint(project, logstore, consumerGroup, shard);
            if (response != null && !response.getCheckPoints().isEmpty()) {
                return response.getCheckPoints().get(0).getCheckPoint();
            }
            return null;
        });
    }

    public ConsumerGroupCheckPointResponse ListCheckpoints(String project, String logstore, String consumerGroup)
            throws Exception {
        return RetryUtil.call(() -> client.GetCheckPoint(project, logstore, consumerGroup));
    }

    public BatchGetLogResponse BatchGetLog(String project, String logstore, int shardId, int count, String cursor,
                                           String endCursor) throws Exception {
        try {
            return RetryUtil.call(() -> client.BatchGetLog(project, logstore, shardId, count, cursor, endCursor));
        } catch (LogException ex) {
            if (ex.GetErrorCode().equals("ShardNotExist")) {
                LOG.warn("Cannot pull log: " + ex.GetErrorMessage());
                return null;
            } else {
                throw ex;
            }
        }
    }

    public GetHistogramsResponse GetHistograms(String project, String logstore, int from, int to, String topic,
                                               String query) throws Exception {
        return RetryUtil.call(() -> client.GetHistograms(project, logstore, from, to, topic, query));
    }

    public GetCursorTimeResponse GetCursorTime(String project, String logstore, int shardId, String cursor)
            throws Exception {
        return RetryUtil.call(() -> client.GetCursorTime(project, logstore, shardId, cursor));
    }
}
