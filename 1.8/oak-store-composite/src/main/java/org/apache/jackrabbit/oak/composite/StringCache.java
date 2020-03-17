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
package org.apache.jackrabbit.oak.composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class caches the path strings used in the CompositeNodeState to avoid
 * keeping too many strings in the memory.
 */
public class StringCache {

    private static final int CACHE_SIZE = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(StringCache.class);

    private final ConcurrentMap<String, String> cache = new ConcurrentHashMap<>(CACHE_SIZE);

    private CompositeNodeStoreMonitor monitor;

    public String get(String path) {
        if (cache.size() >= CACHE_SIZE && !cache.containsKey(path)) {
            LOG.debug("Cache size too big. Revise your mount setup.");
            return path;
        }
        return cache.computeIfAbsent(path, (k) -> {
            if (monitor != null) {
                monitor.onAddStringCacheEntry();
            }
            return path;
        });
    }

    public StringCache withMonitor(CompositeNodeStoreMonitor monitor) {
        this.monitor = monitor;
        return this;
    }
}
