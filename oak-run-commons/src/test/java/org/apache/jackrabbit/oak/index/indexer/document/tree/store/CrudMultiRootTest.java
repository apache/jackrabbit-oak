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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.Test;

/**
 * Create, read, update, delete test.
 */
public class CrudMultiRootTest {

    @Test
    public void memory() {
        Store store = StoreBuilder.build("");
        test(store);
        testAdd(store);
    }

    @Test
    public void file() {
        Store store = StoreBuilder.build(
                "type=file\n" +
                "dir=target/files\n" +
                "cacheSizeMB=1\n" +
                "maxFileSizeBytes=100");
        store.setWriteCompression(Compression.NO);
        test(store);
        testAdd(store);
    }

    @Test
    public void fileLZ4() {
        Store store = StoreBuilder.build(
                "type=file\n" +
                "dir=target/files");
        store.setWriteCompression(Compression.LZ4);
        test(store);
        testAdd(store);
    }

    public void testAdd(Store store) {
        store.removeAll();
        TreeSession session = new TreeSession(store);
        session.init();
        int count = 100;
        TreeMap<String, String> map = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "v" + i);
            session.put("key" + i, "v" + i);
            session.flush();
            if (count % 10 == 0) {
                session.checkpoint();
                session.mergeRoots(10);
            }
        }
        session.mergeRoots(Integer.MAX_VALUE);
        session.flush();

        session = new TreeSession(store);
        TreeMap<String, String> map2 = new TreeMap<>();
        for(String k : session.keys()) {
            map2.put(k, session.get(k));
        }
        assertEquals(map, map2);
    }

    public void test(Store store) {
        store.removeAll();
        TreeSession session = new TreeSession(store);
        session.init();
        int count = 100;
        TreeMap<String, String> verify = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            session.put("hello" + i, "world" + i);
            verify.put("hello" + i, "world" + i);
        }
        session.checkpoint();
        for (int i = 0; i < count; i++) {
            if (i % 3 == 0) {
                session.put("hello" + i, null);
                verify.put("hello" + i, null);
            } else if (i % 3 == 1) {
                session.put("hello" + i, "World" + i);
                verify.put("hello" + i, "World" + i);
            }
        }

        Iterator<Entry<String, String>> it = verify.entrySet().iterator();
        Iterator<String> it2 = session.keys().iterator();
        String previous = null;
        for (int i = 0; i < count; i++) {
            Entry<String, String> e = it.next();
            String k = it2.next();
            assertEquals(e.getKey(), k);
            assertEquals(e.getValue(), session.get(k));

            Iterator<String> it3 = session.keys(previous).iterator();
            assertTrue(it3.hasNext());
            assertEquals("previous: " + previous, k, it3.next());
            Iterator<Entry<String, String>> it4 = session.iterator(previous);
            assertTrue("previous: " + previous, it4.hasNext());
            Entry<String, String> e4 = it4.next();
            assertEquals("previous " + previous, k, e4.getKey());
            assertEquals(e.getValue(), e4.getValue());
            if (it4.hasNext()) {
                Entry<String, String> e4b = it4.next();
                assertFalse("key returned twice " + e4b.getKey(), e4b.getKey().equals(k));
            }

            previous = k;
        }

        session = new TreeSession(store);
        for (int i = 0; i < count; i++) {
            String key = "hello" + i;
            assertEquals("world" + i, session.get(key));
            session.put(key, "World " + i);
        }
        session.flush();

        session = new TreeSession(store);
        for (int i = 0; i < count; i++) {
            String key = "hello" + i;
            assertEquals("World " + i, session.get(key));
            session.put(key, null);
        }
        session.flush();

        session = new TreeSession(store);
        for (int i = 0; i < count; i++) {
            String key = "hello" + i;
            assertEquals(null, session.get(key));
        }

        for (int i = 0; i < 20; i++) {
            session.checkpoint();
        }
    }
}
