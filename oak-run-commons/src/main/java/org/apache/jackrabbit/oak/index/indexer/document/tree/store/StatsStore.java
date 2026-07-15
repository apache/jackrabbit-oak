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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A wrapper store that allows capturing performance counters for a storage
 * backend.
 */
public class StatsStore implements Store {

    private final Properties config;
    private final Store backend;
    private long lastLog;
    private AtomicLong pending = new AtomicLong();

    private final ConcurrentHashMap<String, Stats> map = new ConcurrentHashMap<>();

    public String toString() {
        return "stats(" + backend + ")";
    }

    StatsStore(Store backend) {
        this.config = backend.getConfig();
        this.backend = backend;
    }

    @Override
    public PageFile getIfExists(String key) {
        long start = start();
        long sizeInBytes = 0;
        try {
            PageFile result = backend.getIfExists(key);
            sizeInBytes = result == null ? 0 : result.sizeInBytes();
            return result;
        } finally {
            add("getIfExists", start, sizeInBytes);
        }
    }

    private long start() {
        pending.incrementAndGet();
        return System.nanoTime();
    }

    private void add(String key, long start, long bytes) {
        long now = System.nanoTime();
        pending.decrementAndGet();
        long nanos = now - start;
        Stats stats = map.computeIfAbsent(key, s -> new Stats(key));
        stats.count++;
        stats.nanosMax = Math.max(stats.nanosMax, nanos);
        stats.nanosMin = Math.min(stats.nanosMin, nanos);
        stats.nanosTotal += nanos;
        stats.bytesMax = Math.max(stats.bytesMax, bytes);
        stats.bytesMin = Math.min(stats.bytesMin, nanos);
        stats.bytesTotal += bytes;
        if (lastLog == 0) {
            lastLog = start;
        }
        if (now - lastLog > 10_000_000_000L) {
            TreeMap<String, Stats> sorted = new TreeMap<>(map);
            System.out.print(backend.toString());
            System.out.println(sorted.values().toString() + " pending " + pending);
            lastLog = now;
        }
    }

    @Override
    public void put(String key, PageFile value) {
        long start = start();
        try {
            backend.put(key, value);
        } finally {
            add("put", start, value.sizeInBytes());
        }
    }

    @Override
    public String newFileName() {
        return backend.newFileName();
    }

    @Override
    public Set<String> keySet() {
        return backend.keySet();
    }

    @Override
    public void remove(Set<String> set) {
        backend.remove(set);
    }

    @Override
    public void removeAll() {
        backend.removeAll();
    }

    @Override
    public long getWriteCount() {
        return backend.getWriteCount();
    }

    @Override
    public long getReadCount() {
        return backend.getReadCount();
    }

    @Override
    public void setWriteCompression(Compression compression) {
        backend.setWriteCompression(compression);
    }

    @Override
    public void close() {
        backend.close();
    }

    @Override
    public Properties getConfig() {
        return config;
    }

    @Override
    public boolean supportsByteOperations() {
        return backend.supportsByteOperations();
    }

    @Override
    public byte[] getBytes(String key) {
        long start = start();
        long len = 0;
        try {
            byte[] result = backend.getBytes(key);
            len = result.length;
            return result;
        } finally {
            add("getBytes", start, len);
        }
    }

    @Override
    public void putBytes(String key, byte[] data) {
        long start = start();
        try {
            backend.putBytes(key, data);
        } finally {
            add("putBytes", start, data.length);
        }
    }

    @Override
    public long getMaxFileSizeBytes() {
        return backend.getMaxFileSizeBytes();
    }

    static class Stats {
        final String key;
        long count;
        long bytesMin;
        long bytesMax;
        long bytesTotal;
        long nanosMin;
        long nanosMax;
        long nanosTotal;

        public Stats(String key) {
            this.key = key;
        }

        public String toString() {
            if (count == 0) {
                return "";
            }
            String result = key;
            result += " " + count + " calls";
            if (bytesTotal > 0 && nanosTotal > 0) {
                result += " " + (bytesTotal / count / 1_000_000) + " avgMB" +
                (bytesTotal == 0 ? "" : (" " + ((bytesTotal * 1_000) / nanosTotal) + " MB/s"));
            }
            return result;
        }

    }

}
