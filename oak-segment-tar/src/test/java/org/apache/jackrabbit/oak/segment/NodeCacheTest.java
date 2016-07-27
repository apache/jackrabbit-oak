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

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.segment.NodeCache.newNodeCache;
import static org.apache.jackrabbit.oak.segment.TestUtils.newRecordId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;

public class NodeCacheTest {
    private final Random rnd = new Random();
    private final MemoryStore store = new MemoryStore();

    private final Map<String, RecordId> keys = newHashMap();
    {
        for (int k = 0; k < 10; k++) {
            keys.put("key-" + k, newRecordId(store, rnd));
        }
    }

    public NodeCacheTest() throws IOException {}

    @Test
    public void readWrite() {
        NodeCache cache = newNodeCache(100, 2);

        cache.put("key-0", keys.get("key-0"), 0);
        assertEquals(keys.get("key-0"), cache.get("key-0"));
        cache.put("key-1", keys.get("key-1"), 1);
        assertEquals(keys.get("key-1"), cache.get("key-1"));
        cache.put("key-2", keys.get("key-2"), 2);
        assertNull(cache.get("key-2"));

        assertNull(keys.get("any"));
        assertEquals(2, cache.size());
    }

    @Test
    public void evictLevels() {
        NodeCache cache = newNodeCache(100, 2);

        cache.put("key-0", keys.get("key-0"), 0);
        assertEquals(keys.get("key-0"), cache.get("key-0"));
        cache.put("key-1", keys.get("key-1"), 1);
        assertEquals(keys.get("key-1"), cache.get("key-1"));

        for (int k = 0; k < 100; k++) {
            if (k <= 98) {
                assertEquals(k + 2, cache.size());
            } else {
                assertEquals(k - 98, cache.size());
            }
            cache.put("fill-" + k, newRecordId(store, rnd), 1);
        }

        assertEquals(keys.get("key-0"), cache.get("key-0"));
        assertNull("Keys at level 1 should have been evicted", cache.get("key-1"));
    }
}
