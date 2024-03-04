/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ElasticsearchConnectionFactory implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnectionFactory.class);
    private final ConcurrentMap<ElasticsearchCoordinate, RestHighLevelClient> clientMap = Maps.newConcurrentMap();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public RestHighLevelClient getConnection(ElasticsearchCoordinate esCoord) {
        lock.readLock().lock();
        try {
            if (isClosed.get()) {
                throw new IllegalStateException("Already closed");
            }

            return clientMap.computeIfAbsent(esCoord, elasticsearchCoordinate -> {
                LOG.info("Creating client {}", elasticsearchCoordinate);
                return new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(elasticsearchCoordinate.getHost(),
                                    elasticsearchCoordinate.getPort(),
                                    elasticsearchCoordinate.getScheme())
                    ));
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            isClosed.set(true);
            clientMap.values().forEach(restHighLevelClient -> {
                try {
                    restHighLevelClient.close();
                } catch (IOException e) {
                    LOG.error("Error occurred while closing a connection", e);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }
}
