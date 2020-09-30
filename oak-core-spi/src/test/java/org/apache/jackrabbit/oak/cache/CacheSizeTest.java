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
package org.apache.jackrabbit.oak.cache;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.junit.Test;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;

/**
 * Test the maximum cache size (for the FileCache).
 */
public class CacheSizeTest {

    private static final Weigher<String, FileObj> weigher = new Weigher<String, FileObj>() {
        @Override
        public int weigh(String key, FileObj value) {
            return Math.round(value.length() / (4 * 1024)); // convert to 4 KB blocks
        }
    };

    private HashMap<String, FileObj> files = new HashMap<>();

    @Test
    public void test() throws ExecutionException {

        long maxSize = 20_000_000_000L;

        long size = Math.round(maxSize / (1024L * 4));

        CacheLoader<String, FileObj> cacheLoader = new CacheLoader<String, FileObj>() {

            @Override
            public FileObj load(String key) throws Exception {
                // Fetch from local cache directory and if not found load from
                // backend
                FileObj cachedFile = getFile(key);
                if (cachedFile.exists()) {
                    return cachedFile;
                } else {
                    return loadFile(key);
                }
            }
        };

        CacheLIRS<String, FileObj> cache = new CacheLIRS.Builder<String, FileObj>().
                maximumWeight(size).
                recordStats().
                weigher(weigher).
                segmentCount(1).
                evictionCallback(new EvictionCallback<String, FileObj>() {
                    @Override
                    public void evicted(String key, FileObj cachedFile, RemovalCause cause) {
                        if (cachedFile != null && cachedFile.exists() && cause != RemovalCause.REPLACED) {
                            delete(cachedFile);
                        }
                    }
                }).build();

        for (int i = 0; i < 15; i++) {
            String name = "n" + i;
            long length = 1_000_000_000;
            files.put(name, new FileObj(name, length));
        }
        for (int i = 0; i < 100; i++) {
            String name = "n" + i;
            cache.get(name, () -> cacheLoader.load(name));
        }

    }

    public FileObj getFile(String key) {
        FileObj obj = files.get(key);
        if (obj == null) {
            // doesn't exist
            return new FileObj(key, 0);
        }
        return obj;
    }

    public FileObj loadFile(String key) {
        FileObj f = new FileObj(key, 10_000_000);
        files.put(key, f);
        return f;
    }

    private void delete(FileObj cachedFile) {
        FileObj old = files.remove(cachedFile.getName());
        if (old == null) {
            throw new AssertionError("trying to remove a file that doesn't exist: " + cachedFile);
        }
        long totalLength = getDirectoryLength();
        throw new AssertionError("removing a file: unexpected in this test; total length " + totalLength + " " + old);
    }

    private long getDirectoryLength() {
        long length = 0;
        for(FileObj obj : files.values()) {
            length += obj.length;
        }
        return length;
    }

    class FileObj {

        private final String name;
        private final long length;

        public FileObj(String name, long length) {
            this.name = name;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public boolean exists() {
            return files.containsKey(name);
        }

        public long length() {
            return length;
        }

        public String toString() {
            return name + "/" + length;
        }

    }
}
