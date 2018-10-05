/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.Session;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConcurrentIndexUpdateIT extends AbstractRepositoryTest {

    private static final boolean ENABLED = Boolean.getBoolean(ConcurrentIndexUpdateIT.class.getSimpleName());

    private static final int NUM_WRITERS = 3;
    private static final String TEST_PATH = "/test/folder";

    public ConcurrentIndexUpdateIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @BeforeClass
    public static void before() {
        assumeTrue(ENABLED);
    }

    @Test
    public void updates() throws Exception {
        Node n = getAdminSession().getRootNode();
        for (String name : PathUtils.elements(TEST_PATH)) {
            n = n.addNode(name, "oak:Unstructured");
        }
        getAdminSession().save();

        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<AtomicLong> counters = Lists.newArrayList();
        List<Thread> writers = Lists.newArrayList();
        for (int i = 0; i < NUM_WRITERS; i++) {
            final int id = i;
            final AtomicLong counter = new AtomicLong();
            counters.add(counter);
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Session s = createAdminSession();
                        try {
                            runTest(s.getNode(TEST_PATH), id, counter);
                        } finally {
                            s.logout();
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }

        for (Thread t : writers) {
            t.start();
        }
        while (anyRunning(writers)) {
            List<Long> before = currentValues(counters);
            Thread.sleep(1000);
            List<Long> after = currentValues(counters);
            System.out.println(diff(after, before));
        }

        for (Exception e : exceptions) {
            throw e;
        }
    }

    private List<Long> diff(List<Long> after, List<Long> before) {
        List<Long> diff = Lists.newArrayListWithCapacity(after.size());
        for (int i = 0; i < after.size(); i++) {
            diff.add(after.get(i) - before.get(i));
        }
        return diff;
    }

    private List<Long> currentValues(List<AtomicLong> counters) {
        List<Long> values = Lists.newArrayListWithCapacity(counters.size());
        for (AtomicLong v : counters) {
            values.add(v.get());
        }
        return values;
    }

    private static void runTest(Node n, int id, AtomicLong counter)
            throws Exception {
        for (int i = 0; i < 10000; i++) {
            Node child = n.addNode("node-" + id, "nt:unstructured");
            n.getSession().save();
            counter.incrementAndGet();
            child.remove();
            n.getSession().save();
            counter.incrementAndGet();
        }
    }

    private static boolean anyRunning(Iterable<Thread> threads) {
        for (Thread t : threads) {
            if (t.isAlive()) {
                return true;
            }
        }
        return false;
    }

}
