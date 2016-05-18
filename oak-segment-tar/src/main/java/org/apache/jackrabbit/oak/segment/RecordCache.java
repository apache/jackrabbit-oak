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

package org.apache.jackrabbit.oak.segment;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Supplier;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration, monitoring and management
// add unit tests
// document, nullability
public class RecordCache<T> {
    private final Map<T, RecordId> records;

    public static final <T> Supplier<RecordCache<T>> factory(final int size) {
        return new Supplier<RecordCache<T>>() {
            @Override
            public RecordCache<T> get() {
                return new RecordCache<>(size);
            }
        };
    }

    public static final <T> Supplier<RecordCache<T>> empty() {
        return new Supplier<RecordCache<T>>() {
            @Override
            public RecordCache<T> get() {
                return new RecordCache<T>(0) {
                    @Override
                    public synchronized void put(T key, RecordId value) { }

                    @Override
                    public synchronized RecordId get(T key) { return null; }
                };
            }
        };
    }

    public RecordCache(final int size) {
        records = new LinkedHashMap<T, RecordId>(size * 4 / 3, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<T, RecordId> eldest) {
                return size() >= size;
            }
        };
    }

    public synchronized void put(T key, RecordId value) {
        records.put(key, value);
    }

    public synchronized RecordId get(T key) {
        return records.get(key);
    }
}
