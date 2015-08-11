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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.HybridMapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MapDBMapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.UnsavedModifications.Snapshot.IGNORE;
import static org.junit.Assert.assertEquals;

public class UnsavedModificationsTest {

    private static final String CHARS = "abcdefghijklmnopqrstuvwxyz";

    // OAK-2888
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
                        mod.persist(ns, IGNORE, new ReentrantLock());
                        Thread.sleep(10);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }
        });
        t2.start();

        long end = System.currentTimeMillis() + 5000;
        while (exceptions.isEmpty() && System.currentTimeMillis() < end) {
            Thread.sleep(1000);
        }

        try {
            for (Exception e : exceptions) {
                throw e;
            }

            exceptions.add(new Exception("stop"));
            t1.join();
            t2.join();

        } finally {
            ns.dispose();
            mod.close();
        }
    }

    // OAK-3112
    @Ignore("Long running performance test")
    @Test
    public void performance() throws Exception {
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

        MapFactory factory = new HybridMapFactory();
        final UnsavedModifications pending = new UnsavedModifications(factory);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random(42);
                Set<String> paths = Sets.newHashSet();
                int num = 0;
                for (int i = 0; i < 1000; i++) {
                    String n1 = randomName(random, 20);
                    for (int j = 0; j < 100; j++) {
                        String n2 = randomName(random, 20);
                        for (int k = 0; k < 100; k++) {
                            num++;
                            String n3 = randomName(random, 20);
                            paths.add("/");
                            paths.add(PathUtils.concat("/", n1));
                            paths.add(PathUtils.concat("/", n1, n2));
                            paths.add(PathUtils.concat("/", n1, n2, n3));
                            if (paths.size() >= 20) {
                                Revision r = new Revision(num, 0, 1);
                                for (String p : paths) {
                                    pending.put(p, r);
                                }
                                paths.clear();
                            }
                            if (random.nextFloat() < 0.00005) {
                                pending.persist(ns, IGNORE, new ReentrantLock());
                            }
                        }
                    }
                }
            }
        });
        t.start();

        Set<String> keys = Sets.newHashSet();
        while (t.isAlive()) {
            keys.clear();
            Stopwatch sw = Stopwatch.createStarted();
            keys.addAll(pending.getPaths());
            sw.stop();
            System.out.println(keys.size() + " keys in " + sw);
            keys.clear();
            Thread.sleep(1000);
        }
        pending.close();
        ns.dispose();
    }

    @Test
    public void getPathsAfterPersist() throws Exception {
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

        MapFactory factory = new HybridMapFactory();
        UnsavedModifications pending = new UnsavedModifications(factory);

        Revision r = new Revision(1, 0, 1);
        for (int i = 0; i <= UnsavedModifications.IN_MEMORY_SIZE_LIMIT; i++) {
            pending.put("/node-" + i, r);
        }

        // start iterating over paths
        Iterator<String> paths = pending.getPaths().iterator();
        for (int i = 0; i < 1000; i++) {
            paths.next();
        }

        // drain pending, this will force it back to in-memory
        pending.persist(ns, IGNORE, new ReentrantLock());

        // loop over remaining paths
        while (paths.hasNext()) {
            paths.next();
        }
        pending.close();
    }

    @Test
    public void getPathsWithRevisionAfterPersist() throws Exception {
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

        MapFactory factory = new HybridMapFactory();
        UnsavedModifications pending = new UnsavedModifications(factory);

        Revision r = new Revision(1, 0, 1);
        for (int i = 0; i <= UnsavedModifications.IN_MEMORY_SIZE_LIMIT; i++) {
            pending.put("/node-" + i, r);
        }

        // start iterating over paths
        Iterator<String> paths = pending.getPaths(r).iterator();
        for (int i = 0; i < 1000; i++) {
            paths.next();
        }

        // drain pending, this will force it back to in-memory
        pending.persist(ns, IGNORE, new ReentrantLock());

        // loop over remaining paths
        while (paths.hasNext()) {
            paths.next();
        }
        pending.close();
    }

    @Test
    public void defaultMapFactoryCreate() {
        MapFactory factory = MapFactory.createFactory();
        assertEquals(MapDBMapFactory.class, factory.getClass());
        factory.dispose();
    }

    @Test
    public void useHybridMapFactory() {
        System.setProperty("oak.useHybridMapFactory", "true");
        MapFactory factory = MapFactory.createFactory();
        assertEquals(HybridMapFactory.class, factory.getClass());
        factory.dispose();
        System.clearProperty("oak.useHybridMapFactory");
    }

    @Test
    public void useMemoryMapFactory() {
        System.setProperty("oak.useMemoryMapFactory", "true");
        MapFactory factory = MapFactory.createFactory();
        assertEquals(MapFactory.DEFAULT.getClass(), factory.getClass());
        factory.dispose();
        System.clearProperty("oak.useMemoryMapFactory");
    }

    private static String randomName(Random r, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(CHARS.charAt(r.nextInt(CHARS.length())));
        }
        return sb.toString();
    }
}
