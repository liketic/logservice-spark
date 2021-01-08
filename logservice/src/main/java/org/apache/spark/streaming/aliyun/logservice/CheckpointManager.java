package org.apache.spark.streaming.aliyun.logservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.spark_project.jetty.util.BlockingArrayQueue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CheckpointManager implements Serializable {
    private static final Log LOG = LogFactory.getLog(CheckpointManager.class);

    private final LoghubClientAgent client;
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final long commitInterval;
    private ExecutorService executorService;
    private long lastCommitAt = -1;
    private final Lock lock = new ReentrantLock();
    private Map<Integer, String> offsetCache = new HashMap<>();
    private ZkHelper zkHelper;

    public CheckpointManager(String project,
                             String logstore,
                             String consumerGroup,
                             LoghubClientAgent client,
                             long commitInterval,
                             ZkHelper zkHelper) {
        this.project = project;
        this.logstore = logstore;
        this.consumerGroup = consumerGroup;
        this.client = client;
        this.commitInterval = commitInterval;
        this.zkHelper = zkHelper;
    }

    public void start() {
        executorService = new ThreadPoolExecutor(4,
                16,
                60,
                TimeUnit.SECONDS,
                new BlockingArrayQueue<>(1024),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        lastCommitAt = System.currentTimeMillis();
    }

    public void commit(List<OffsetRange> offsetRanges) {
        if (offsetRanges.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            for (OffsetRange offset : offsetRanges) {
                int shard = offset.shardId();
                String cursor = offset.fromCursor();
                String previous = offsetCache.get(shard);
                if (previous != null && ShardUtils.checkCursorLessThan(cursor, previous)) {
                    LOG.warn("Shard " + shard + " offset " + cursor + " was less than previous: " + previous);
                } else {
                    offsetCache.put(shard, cursor);
                }
            }
            flushIfNeeded(false);
        } catch (Exception ex) {
            LOG.error("Error committing offset", ex);
        } finally {
            lock.unlock();
        }
        LOG.info("Cleaning up RDD cache..");
        for (OffsetRange item : offsetRanges) {
            executorService.submit(() -> deleteRDDSafe(item.rddID(), item.shardId()));
        }
        LOG.info("Cleaning up RDD cache succeed!");
    }

    private void deleteRDDSafe(int id, int shardId) {
        try {
            zkHelper.cleanupRDD(id, shardId);
        } catch (Exception ex) {
            LOG.error("Error while deleting RDD file", ex);
        }
    }

    private void commitAll() {
        Map<Integer, String> copy;
        lock.lock();
        copy = offsetCache;
        offsetCache = new HashMap<>();
        lock.unlock();
        try {
            for (Map.Entry<Integer, String> offset : copy.entrySet()) {
                executorService.submit(() -> {
                    try {
                        client.safeUpdateCheckpoint(project, logstore, consumerGroup, offset.getKey(), offset.getValue());
                        LOG.info("Update offset [" + offset.getKey() + "] " + offset.getValue() + " succeed!");
                    } catch (Exception ex) {
                        LOG.error("Error updating checkpoint", ex);
                    }
                });
            }
        } catch (Exception ex) {
            LOG.error("Error flushing checkpoint to SLS server", ex);
        }
    }

    public void flushIfNeeded(boolean force) {
        long now = System.currentTimeMillis();
        if (force || now - lastCommitAt >= commitInterval) {
            commitAll();
            lastCommitAt = now;
            LOG.info("Flush offsets succeed!");
        }
    }

    public void close() {
        flushIfNeeded(true);
        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException ex) {
            LOG.warn("InterruptedException caught!", ex);
            Thread.currentThread().interrupt();
        }
    }
}
