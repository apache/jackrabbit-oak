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
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for OAK-2888
 */
public class UnsavedModificationsTest {

    @Ignore
    @Test
    public void concurrent() throws Exception {
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> void update(Collection<T> collection,
                                                    List<String> keys,
                                                    UpdateOp updateOp) {
                // ignore call
            }
        };
        final DocumentNodeStore ns = new DocumentMK.Builder().setDocumentStore(store)
                .getNodeStore();
        final UnsavedModifications mod = new UnsavedModifications();

        Thread t1 = new Thread(new Runnable() {
            private final Random rand = new Random();
            @Override
            public void run() {
                while (exceptions.isEmpty()) {
                    int num = rand.nextInt(100) + 100;
                    try {
                        addPaths(num);
                        Thread.sleep(1);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }

            private void addPaths(int num) {
                Revision r = Revision.newRevision(1);
                while (num-- > 0) {
                    String path = "";
                    int d = rand.nextInt(5);
                    for (int i = 0; i < d; i++) {
                        path += "/node-" + rand.nextInt(10);
                    }
                    if (path.length() == 0) {
                        path = "/";
                    }

                    mod.put(path, r);
                }
            }
        });
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (exceptions.isEmpty()) {
                    try {
                        mod.persist(ns, new ReentrantLock());
                        Thread.sleep(10);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }
        });
        t2.start();

        while (exceptions.isEmpty()) {
            Thread.sleep(1000);
            System.out.println("size: " + mod.getPaths().size());
        }

        try {
            for (Exception e : exceptions) {
                throw e;
            }
        } finally {
            ns.dispose();
            mod.close();
        }
    }

}
