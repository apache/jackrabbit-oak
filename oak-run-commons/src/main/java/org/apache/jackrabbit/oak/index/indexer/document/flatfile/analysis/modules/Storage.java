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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory storage for collectors.
 *
 * It allows to measure the amount of memory used.
 */
public class Storage {

    private long storageSize;
    private final TreeMap<String, Long> data = new TreeMap<>();

    public Long get(String key) {
        return data.get(key);
    }

    public long add(String key, long value) {
        Long old = data.get(key);
        long newValue;
        if (old == null) {
            newValue = value;
            storageSize += key.length() + 8;
        } else {
            newValue = old + value;
        }
        data.put(key, newValue);
        return newValue;
    }

    public void put(String key, long value) {
        Long old = data.put(key, value);
        if (old == null) {
            storageSize += key.length() + 8;
        }
    }

    public Set<Entry<String, Long>> entrySet() {
        return data.entrySet();
    }

    public String toString() {
        return "storage size: " + (storageSize / 1024 / 1024) + " MB; " + data.size() + " entries\n";
    }

    /**
     * Return the storage estimated size, in bytes.
     */
    public long getStorageSize() {
        return storageSize;
    }

    /**
     * Get the number of entries.
     */
    public int size() {
        return data.size();
    }

}
