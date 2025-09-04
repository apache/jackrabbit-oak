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

import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

public class MergeRootsTest {

    @Test
    public void gcTest() {
        Store store = StoreBuilder.build("");
        TreeSession session = new TreeSession(store);
        session.init();
        session.runGC();
    }

    @Test
    public void simpleTest() {
        Store store = StoreBuilder.build("");
        TreeSession session = new TreeSession(store);
        session.init();
        for (int i = 0; i < 10; i++) {
            session.put("x" + i, "y" + i);
            session.checkpoint();
            assertEquals(i + 2, session.getRootCount());
        }
        assertEquals(11, session.getRootCount());
        session.mergeRoots(2);
        assertEquals(10, session.getRootCount());
        for (int i = 0; i < 10; i++) {
            assertEquals("y" + i, session.get("x" + i));
        }
    }

    @Test
    public void multipleRootAppendTest() {
        Store store = StoreBuilder.build("");
        TreeSession session = new TreeSession(store);
        session.init();
        for(int j = 1; j <= 3; j++) {
            for (int i = 0; i < 10 * j; i++) {
                session.put("x" + i, "y" + i + "j" + j);
            }
            session.checkpoint();
        }
        for (int i = 0; i < 10 * 3; i++) {
            if (!("y" + i + "j3").equals(session.get("x" + i))) {
                assertEquals("y" + i + "j3", session.get("x" + i));
            }
        }
        session.mergeRoots(Integer.MAX_VALUE);
        for (int i = 0; i < 10 * 3; i++) {
            if (!("y" + i + "j3").equals(session.get("x" + i))) {
                assertEquals("y" + i + "j3", session.get("x" + i));
            }
        }
    }

    @Test
    public void multipleRootRandomOverwriteTest() {
        Store store = StoreBuilder.build("");
        TreeSession session = new TreeSession(store);
        session.init();
        TreeMap<String, String> map = new TreeMap<>();
        Random r = new Random(42);
        for(int j = 1; j <= 3; j++) {
            for (int i = 0; i < 10; i++) {
                String k = "x" + r.nextInt(30);
                String v = "y" + i + "j" + j;
                session.put(k, v);
                map.put(k, v);
            }
            session.checkpoint();
        }
        session.mergeRoots(Integer.MAX_VALUE);
        for(Entry<String, String> e : map.entrySet()) {
            assertEquals(e.getValue(), session.get(e.getKey()));
        }
        TreeMap<String, String> map2 = new TreeMap<>();

        for(Entry<String, String> e : map.entrySet()) {
            map2.put(e.getKey(), e.getValue());
        }
        assertEquals(map2, map);
    }

    @Test
    public void logStructuredMerge() {
        Store store = StoreBuilder.build(
                "type=memory\n" +
                "maxFileSizeBytes=10000");
        TreeSession session = new TreeSession(store);
        session.init();
        for (int batch = 1; batch <= 200; batch++) {
            // System.out.println("batch " + batch);
            if (batch % 10 == 0) {
                session.mergeRoots(10);
                // System.out.println("merged 10 roots; new root count: " + session.getRootCount());
            }
            if (batch % 100 == 0) {
                session.mergeRoots(20);
                // System.out.println("merged 20 roots; new root count: " + session.getRootCount());
                // System.out.println(session.getInfo());
            }
            session.flush();
            // System.out.println("info:");
            // System.out.println(session.getInfo());
            session.runGC();
            for (int j = 0; j < 100; j++) {
                session.put("x" + j + " " + Math.random(), new String(new char[1000]));
            }
            session.checkpoint();
        }
        assertEquals(3, session.getRootCount());
        session.mergeRoots(Integer.MAX_VALUE);
        assertEquals(1, session.getRootCount());
    }


}
