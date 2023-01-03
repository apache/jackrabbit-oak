/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.SegmentCacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

public class PersistentRedisCache extends AbstractPersistentCache {
    private static final Logger logger = LoggerFactory.getLogger(PersistentRedisCache.class);
    public static final int DEFAULT_REDIS_CACHE_EXPIRE_SECONDS = 3600 * 24 * 2;
    public static final String NAME = "Segment Redis Cache";

    private static final String REDIS_PREFIX = "SEGMENT";
    private final IOMonitor redisCacheIOMonitor;
    private final JedisPool redisPool;
    private final SetParams setParamsWithExpire;

    public PersistentRedisCache(String redisHost, int redisPort, int redisExpireSeconds, int redisSocketTimeout,
            int redisConnectionTimeout, int redisMinConnections, int redisMaxConnections, int redisMaxTotalConnections,
            int redisDBIndex, IOMonitor redisCacheIOMonitor) {
        this.redisCacheIOMonitor = redisCacheIOMonitor;
        int redisExpireSeconds1 = redisExpireSeconds < 0 ? DEFAULT_REDIS_CACHE_EXPIRE_SECONDS : redisExpireSeconds;
        setParamsWithExpire = SetParams.setParams().ex(redisExpireSeconds1);

        if (redisPort == 0) {
            redisPort = 6379;
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setMaxWaitMillis(redisSocketTimeout);
        jedisPoolConfig.setMinIdle(redisMinConnections);
        jedisPoolConfig.setMaxIdle(redisMaxConnections);
        jedisPoolConfig.setMaxTotal(redisMaxTotalConnections);

        this.redisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, redisConnectionTimeout,
                redisSocketTimeout, null, redisDBIndex, null);
        this.segmentCacheStats = new SegmentCacheStats(NAME, this::getRedisMaxMemory, this::getCacheElementCount,
                this::getCurrentWeight, this::getNumberOfEvictedKeys);
    }

    private long getCacheElementCount() {
        try(Jedis redis = redisPool.getResource()) {
            return redis.dbSize();
        } catch (JedisException e) {
            logger.error("Error getting number of elements in redis", e);
        }

        return -1;
    }

    private long getRedisMaxMemory() {
        try{
            return Long.parseLong(getRedisProperty("memory",   "maxmemory"));
        } catch (JedisException | IOException e) {
            logger.error("Error getting redis configuration value for 'maxmemory'", e);
        }
        return -1;
    }

    private long getCurrentWeight() {
        try{
            return Long.parseLong(getRedisProperty("memory",   "used_memory"));
        } catch (JedisException | IOException e) {
            logger.error("Error getting number of elements in redis", e);
        }
        return -1;
    }

    private long getNumberOfEvictedKeys() {
        try{
            return Long.parseLong(getRedisProperty("stats",   "evicted_keys"));
        } catch (JedisException | IOException e) {
            logger.error("Error getting number of evicted elements in redis", e);
        }
        return -1;
    }

    private String getRedisProperty(String section, String propertyName) throws IOException {
        try(Jedis redis = redisPool.getResource()) {
            String redisInfoString = redis.info(section);
            Properties props = new Properties();
            props.load(new StringReader(redisInfoString));
            return (String) props.get(propertyName);
        }
    }

    @Override
    protected Buffer readSegmentInternal(long msb, long lsb) {
        String segmentId = new UUID(msb, lsb).toString();

        Stopwatch stopwatch = Stopwatch.createStarted();
        try(Jedis redis = redisPool.getResource()) {
            redisCacheIOMonitor.beforeSegmentRead(null, msb, lsb, 0);

            final byte[] bytes = redis.get((REDIS_PREFIX + ":" + segmentId).getBytes());
            if (bytes != null) {
                long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                redisCacheIOMonitor.afterSegmentRead(null, msb, lsb, bytes.length, elapsed);

                Buffer buffer;
                if (OFF_HEAP) {
                    buffer = Buffer.allocateDirect(bytes.length);
                } else {
                    buffer = Buffer.allocate(bytes.length);
                }
                buffer.put(bytes);
                buffer.flip();
                return buffer;
            }
        } catch (Exception e) {
            logger.error("Error loading segment {} from cache", segmentId, e);
        }

        return null;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        String segmentId = new UUID(msb, lsb).toString();

        try (Jedis redis = redisPool.getResource()) {
            return redis.exists((REDIS_PREFIX + ":" + segmentId).getBytes());
        } catch (JedisException e) {
            logger.error("Error checking segment existence {} in cache: {}", segmentId, e);
        }

        return false;
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer) {
        String segmentId = new UUID(msb, lsb).toString();
        Buffer bufferCopy = buffer.duplicate();

        Runnable task = () -> {
            if (writesPending.add(segmentId)) {
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try (WritableByteChannel channel = Channels.newChannel(bos); Jedis redis = redisPool.getResource()) {
                    while (bufferCopy.hasRemaining()) {
                        bufferCopy.write(channel);
                    }
                    final byte[] key = (REDIS_PREFIX + ":" + segmentId).getBytes();
                    redis.set(key, bos.toByteArray(), setParamsWithExpire);
                    cacheSize.addAndGet(bos.size());
                } catch (Throwable t) {
                    logger.debug("Unable to write segment {} to cache: {}", segmentId, t.getMessage());
                } finally {
                    writesPending.remove(segmentId);
                }
            }
        };

        executor.execute(task);
    }

    @Override
    public void cleanUp() {
        // do nothing
    }
}
