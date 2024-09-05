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

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

/**
 * A wrapper for storage backends that allows to log store and read operations.
 */
public class LogStore implements Store {

    private final Properties config;
    private final Store backend;

    public String toString() {
        return "log(" + backend + ")";
    }

    LogStore(Store backend) {
        this.config = backend.getConfig();
        this.backend = backend;
    }

    private void log(String message, Object... args) {
        System.out.println(backend + "." + message + " " + Arrays.toString(args));
    }

    @Override
    public PageFile getIfExists(String key) {
        log("getIfExists", key);
        return backend.getIfExists(key);
    }

    @Override
    public void put(String key, PageFile value) {
        log("put", key);
        backend.put(key, value);
    }

    @Override
    public String newFileName() {
        return backend.newFileName();
    }

    @Override
    public Set<String> keySet() {
        log("keySet");
        return backend.keySet();
    }

    @Override
    public void remove(Set<String> set) {
        log("remove", set);
        backend.remove(set);
    }

    @Override
    public void removeAll() {
        log("removeAll");
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
        log("setWriteCompression", compression);
        backend.setWriteCompression(compression);
    }

    @Override
    public void close() {
        log("close");
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
        log("getBytes", key);
        return backend.getBytes(key);
    }

    @Override
    public void putBytes(String key, byte[] data) {
        log("putBytes", key, data.length);
        backend.putBytes(key, data);
    }

    @Override
    public long getMaxFileSizeBytes() {
        return backend.getMaxFileSizeBytes();
    }

}