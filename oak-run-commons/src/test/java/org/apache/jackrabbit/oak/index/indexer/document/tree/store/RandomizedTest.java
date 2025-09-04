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
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Random;

import org.junit.Test;

public class RandomizedTest {

    @Test
    public void test() {
        Store store = StoreBuilder.build(
                "type=memory\n" +
                "cacheSizeMB=1\n" +
                "maxFileSizeBytes=100");
        TreeSession session = new TreeSession(store);
        session.init();
        HashMap<String, String> verify = new HashMap<>();
        Random r = new Random(1);
        boolean verifyAllAlways = false;

        int count = 100000;
        int size = 100;
        for (int i = 0; i < count; i++) {
            String key = "" + r.nextInt(size);
            String value;
            boolean remove = r.nextBoolean();
            if (remove) {
                value = null;
            } else {
                value = "x" + r.nextInt(size);
            }
            verify(verify, session, key);
            log("#" + i + " put " + key + "=" + (value == null ? "null" : value.toString().replace('\n', ' ')));
            verify.put(key, value);
            session.put(key, value);
            verify(verify, session, key);
            if (r.nextInt(size) == 0) {
                log("flush");
                session.flush();
            }
            if (verifyAllAlways) {
                for(String k : verify.keySet()) {
                    verify(verify, session, k);
                }
            }
        }

        String min = session.getMinKey();
        assertEquals("0", min);
        String max = session.getMaxKey();
        assertEquals("99", max);
        Random r1 = new Random(42);
        String median = session.getApproximateMedianKey(min, max, r1);
        assertEquals("85", median);
    }

    private void verify(HashMap<String, String> verify, TreeSession session, String key) {
        String a = verify.get(key);
        String b = session.get(key);
        if (a == null || b == null) {
            if (a != null) {
                fail(key + " a: " + a + " b: " + b);
            }
        } else {
            assertEquals(
                    key + " a: " + a + " b: " + b,
                    a.toString(), b.toString());
        }
    }

    private void log(String msg) {
        // System.out.println(msg);
    }
}
