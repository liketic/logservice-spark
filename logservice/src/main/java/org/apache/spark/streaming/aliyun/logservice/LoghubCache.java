package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.openservices.log.common.Shard;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoghubCache {
    private static final Log LOG = LogFactory.getLog(LoghubCache.class);

    private final LoghubClientAgent client;
    private final long cacheTime;


    private static class CacheItem<T> {
        private T item;
        private long expireTime;

        public CacheItem(T item,
                         long expireTime) {
            this.item = item;
            this.expireTime = expireTime;
        }

        public boolean isExpired(long currentTime) {
            return expireTime < currentTime;
        }
    }

    private final Map<String, CacheItem<List<Shard>>> shardCache = new ConcurrentHashMap<>();


    public LoghubCache(LoghubClientAgent client,
                       long cacheTime) {
        this.client = client;
        this.cacheTime = cacheTime;
    }

    public List<Shard> listShard(String project, String logstore) {
        String key = project + "#" + logstore;
        CacheItem<List<Shard>> cacheItem = shardCache.get(key);
        long currentTime = System.currentTimeMillis();
        if (cacheItem != null && !cacheItem.isExpired(currentTime)) {
            return cacheItem.item;
        }
        try {
            List<Shard> shards = client.ListShard(project, logstore).GetShards();
            cacheItem = new CacheItem<>(shards, currentTime + cacheTime);
            shardCache.put(key, cacheItem);
            return shards;
        } catch (Exception ex) {
            LOG.error("Error listing shards", ex);
        }
        return cacheItem != null ? cacheItem.item : Collections.emptyList();
    }
}
