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
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Performs non-conflicting property updates on DocumentMK with multiple threads.
 */
public class ConcurrentUpdatesTest extends AbstractMongoConnectionTest {

    private static final int NUM_WRITERS = 10;

    private static final int NUM_OPS = 100;

    @Test
    public void test() throws Exception {
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        List<Thread> writers = new ArrayList<Thread>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < NUM_WRITERS; i++) {
            final String nodeName = "test-" + i;
            sb.append("+\"").append(nodeName).append("\":{}");
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < NUM_OPS; i++) {
                            mk.commit("/" + nodeName, "^\"prop\":" + i, null, null);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        mk.commit("/", sb.toString(), null, null);
        long time = System.currentTimeMillis();
        for (Thread t : writers) {
            t.start();
        }
        for (Thread t : writers) {
            t.join();
        }
        time = System.currentTimeMillis() - time;
        // System.out.println(time);
        for (Exception e : exceptions) {
            throw e;
        }
    }
}
