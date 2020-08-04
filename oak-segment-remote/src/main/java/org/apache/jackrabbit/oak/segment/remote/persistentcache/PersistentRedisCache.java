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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class PersistentRedisCache extends AbstractPersistentCache {
    private static final Logger logger = LoggerFactory.getLogger(PersistentRedisCache.class);
    public static final int DEFAULT_REDIS_CACHE_EXPIRE_SECONDS = 3600 * 24 * 2;

    private static final String REDIS_PREFIX = "SEGMENT";
    private final int redisExpireSeconds;
    private JedisPool redisPool;

    public PersistentRedisCache(String redisHost, int redisPort, int redisExpireSeconds, int redisSocketTimeout, int redisConnectionTimeout,
                                int redisMinConnections, int redisMaxConnections, int redisMaxTotalConnections) {
        this.redisExpireSeconds = redisExpireSeconds < 0 ? DEFAULT_REDIS_CACHE_EXPIRE_SECONDS : redisExpireSeconds;

        if (redisPort == 0) {
            redisPort = 6379;
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setMaxWaitMillis(redisSocketTimeout);
        jedisPoolConfig.setMinIdle(redisMinConnections);
        jedisPoolConfig.setMaxIdle(redisMaxConnections);
        jedisPoolConfig.setMaxTotal(redisMaxTotalConnections);

        this.redisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, redisConnectionTimeout, redisSocketTimeout, null, Protocol.DEFAULT_DATABASE, null);
    }

    @Override
    public Buffer readSegment(long msb, long lsb) {
        String segmentId = new UUID(msb, lsb).toString();

        try(Jedis redis = redisPool.getResource()) {
            final byte[] bytes = redis.get((REDIS_PREFIX + ":" + segmentId).getBytes());
            if (bytes != null) {
                Buffer buffer = Buffer.allocateDirect(bytes.length);

                buffer.put(bytes);
                buffer.flip();
                return buffer;
            }
        } catch (JedisException e) {
            logger.error("Error loading segment {} from cache: {}", segmentId, e);
        }

        if (nextCache != null) {
            return nextCache.readSegment(msb, lsb);
        }

        return null;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        String segmentId = new UUID(msb, lsb).toString();

        try(Jedis redis = redisPool.getResource()) {
            return redis.exists((REDIS_PREFIX + ":" + segmentId).getBytes());
        } catch (JedisException e) {
            logger.error("Error checking segment existence {} in cache: {}", segmentId, e);
        }

        return false;
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer){
        String segmentId = new UUID(msb, lsb).toString();
        Buffer bufferCopy = buffer.duplicate();

        Runnable task = () -> {
            if (lockSegmentWrite(segmentId)) {
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try (WritableByteChannel channel = Channels.newChannel(bos); Jedis redis = redisPool.getResource()) {
                    while (bufferCopy.hasRemaining()) {
                        bufferCopy.write(channel);
                    }
                    final byte[] key = (REDIS_PREFIX + ":" + segmentId).getBytes();
                    redis.set(key, bos.toByteArray());
                    redis.expire(key, redisExpireSeconds);
                    cacheSize.addAndGet(bos.size());
                } catch (Throwable t) {
                    logger.error("Error writing segment {} to cache: {}", segmentId, t);
                } finally {
                    unlockSegmentWrite(segmentId);
                }
            }
        };

        executor.execute(task);

        if (nextCache != null) {
            nextCache.writeSegment(msb, lsb, buffer);
        }
    }

    @Override
    public void cleanUp() {
        // do nothing
    }
}
