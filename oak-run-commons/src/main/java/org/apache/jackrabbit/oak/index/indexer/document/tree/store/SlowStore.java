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

/**
 * A wrapper store to simulate a slow backend store. It can be used for
 * simulations. It is not intended to be used for real-world situations.
 */
public class SlowStore implements Store {

    private final Properties config;
    private final Store backend;
    private final long mbOverhead;
    private final long mbPerSecond;

    public String toString() {
        return "slow(" + backend + ")";
    }

    SlowStore(Store backend) {
        this.config = backend.getConfig();
        this.backend = backend;
        this.mbOverhead = Integer.parseInt(config.getProperty("mbOverhead", "3"));
        this.mbPerSecond = Integer.parseInt(config.getProperty("mbPerSecond", "70"));
    }

    private void delay(long nanos, int sizeInBytes) {
        long bytes = sizeInBytes + 1_000_000 * mbOverhead;
        long nanosRequired = 1_000 * bytes / mbPerSecond;
        long delayNanos = nanosRequired - nanos;
        long delayMillis = delayNanos / 1_000_000;
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public PageFile getIfExists(String key) {
        long start = System.nanoTime();
        PageFile result = backend.getIfExists(key);
        long time = System.nanoTime() - start;
        delay(time, result == null ? 0 : result.sizeInBytes());
        return result;
    }

    @Override
    public void put(String key, PageFile value) {
        long start = System.nanoTime();
        backend.put(key, value);
        long time = System.nanoTime() - start;
        delay(time, value.sizeInBytes());
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
        long start = System.nanoTime();
        byte[] result = backend.getBytes(key);
        long time = System.nanoTime() - start;
        delay(time, result.length);
        return result;
    }

    @Override
    public void putBytes(String key, byte[] data) {
        long start = System.nanoTime();
        backend.putBytes(key, data);
        long time = System.nanoTime() - start;
        delay(time, data.length);
    }

    @Override
    public long getMaxFileSizeBytes() {
        return backend.getMaxFileSizeBytes();
    }

}
