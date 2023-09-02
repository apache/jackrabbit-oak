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
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements Store {

    private final Properties config;
    private final ConcurrentHashMap<String, PageFile> map = new ConcurrentHashMap<>();
    private long nextFileName;
    private long writeCount, readCount;

    public MemoryStore() {
        this(new Properties());
    }

    public MemoryStore(Properties config) {
        this.config = config;
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
        if (!key.equals(file.getFileName())) {
            throw new AssertionError("key: " + key + " file name: " + file.getFileName());
        }
        if (key.equals(file.getNextRoot())) {
            throw new AssertionError("key: " + key + " nextRoot: " + file.getNextRoot());
        }
        writeCount++;
        map.put(key, file);
    }

    public boolean putIfAbsent(String key, PageFile value) {
        writeCount++;
        return map.putIfAbsent(key, value) == null;
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

}
