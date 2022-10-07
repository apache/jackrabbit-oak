/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.run;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Generic concurrent file downloader which uses Java NIO channels to potentially leverage OS internal optimizations.
 */
public class Downloader implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Downloader.class);

    private final ExecutorService executorService;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final List<Future<ItemResponse>> responses;

    public Downloader(int concurrency, int connectTimeoutMs, int readTimeoutMs) {
        if (concurrency <= 0 || concurrency > 1000) {
            throw new IllegalArgumentException("concurrency range must be between 1 and 1000");
        }
        if (connectTimeoutMs < 0 || readTimeoutMs < 0) {
            throw new IllegalArgumentException("connect and/or read timeouts can not be negative");
        }
        LOG.info("Initializing Downloader with max number of concurrent requests={}", concurrency);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.executorService = new ThreadPoolExecutor(
                (int) Math.ceil(concurrency * .1), concurrency, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("downloader-%d")
                        .setDaemon(true)
                        .build()
        );
        this.responses = new ArrayList<>();
    }

    public void offer(Item item) {
        Callable<ItemResponse> callableTask = () -> {
            ItemResponse response = new ItemResponse(item);
            long t0 = System.nanoTime();
            try {
                URLConnection sourceUrl = new URL(item.source).openConnection();
                sourceUrl.setConnectTimeout(Downloader.this.connectTimeoutMs);
                sourceUrl.setReadTimeout(Downloader.this.readTimeoutMs);

                Path destinationPath = Paths.get(item.destination);
                Files.createDirectories(destinationPath.getParent());
                try (ReadableByteChannel byteChannel = Channels.newChannel(sourceUrl.getInputStream());
                     FileOutputStream outputStream = new FileOutputStream(destinationPath.toFile())) {
                    response.size = outputStream.getChannel()
                            .transferFrom(byteChannel, 0, sourceUrl.getContentLengthLong());
                }
            } catch (Exception e) {
                response.failed = true;
                response.throwable = e;
            } finally {
                response.time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            }
            return response;
        };
        responses.add(this.executorService.submit(callableTask));
    }

    public List<ItemResponse> waitUntilComplete() {
        return responses.stream()
                .map(itemResponseFuture -> {
                    try {
                        return itemResponseFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }

    public static class Item {
        public String source;
        public String destination;

        @Override
        public String toString() {
            return "Item{" +
                    "source='" + source + '\'' +
                    ", destination='" + destination + '\'' +
                    '}';
        }
    }

    public static class ItemResponse {
        public final Item item;
        public boolean failed;
        public long size;
        public long time;
        public Throwable throwable;

        public ItemResponse(Item item) {
            this.item = item;
        }
    }

}
