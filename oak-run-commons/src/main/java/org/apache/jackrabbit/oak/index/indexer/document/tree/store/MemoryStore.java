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

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

/**
 * An in-memory storage backend for the tree store.
 */
public class MemoryStore implements Store {

    private final Properties config;
    private final HashMap<String, PageFile> map = new HashMap<>();
    private long nextFileName;
    private long writeCount, readCount;
    private final long maxFileSizeBytes;

    public MemoryStore() {
        this(new Properties());
    }

    public MemoryStore(Properties config) {
        this.config = config;
        this.maxFileSizeBytes = Long.parseLong(config.getProperty(
                Store.MAX_FILE_SIZE_BYTES, "" + Store.DEFAULT_MAX_FILE_SIZE_BYTES));
    }

    @Override
    public void setWriteCompression(Compression compression) {
        // ignore
    }

    public PageFile getIfExists(String key) {
        readCount++;
        return map.get(key);
    }

    public void put(String key, PageFile file) {
        writeCount++;
        map.put(key, file);
    }

    public String toString() {
        return "files: " + map.size();
    }

    public String newFileName() {
        return "f" + nextFileName++;
    }

    public Set<String> keySet() {
        return map.keySet();
    }

    public void remove(Set<String> set) {
        for (String key : set) {
            writeCount++;
            map.remove(key);
        }
    }

    @Override
    public void removeAll() {
        map.clear();
        nextFileName = 0;
    }

    @Override
    public long getWriteCount() {
        return writeCount;
    }

    @Override
    public long getReadCount() {
        return readCount;
    }

    @Override
    public void close() {
    }

    @Override
    public Properties getConfig() {
        return config;
    }

    @Override
    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

}
