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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * <code>ConcurrentConflictTest</code>...
 */
public class ConcurrentConflictTest extends BaseMongoMKTest {

    private static final boolean USE_LOGGER = true;
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentConflictTest.class);
    private static final int NUM_WRITERS = 3;
    private static final int NUM_NODES = 10;
    private static final int NUM_TRANSFERS_PER_THREAD = 10;
    private DocumentStore store;
    private List<MongoMK> kernels = new ArrayList<MongoMK>();
    private final StringBuilder logBuffer = new StringBuilder();

    @Before
    @Override
    public void initMongoMK() {
        logBuffer.setLength(0);
        this.store = new MemoryDocumentStore();
        MongoMK mk = openMongoMK();
        for (int i = 0; i < NUM_NODES; i++) {
            mk.commit("/", "+\"node-" + i + "\":{\"value\":100}", null, null);
        }
        mk.dispose();
        for (int i = 0; i < NUM_WRITERS; i++) {
            kernels.add(openMongoMK());
        }
    }

    private MongoMK openMongoMK() {
        return new MongoMK.Builder().setAsyncDelay(10).setDocumentStore(store).open();
    }

    @Ignore
    @Test
    public void concurrentUpdatesWithBranch() throws Exception {
        concurrentUpdates(true);
    }

    @Ignore
    @Test
    public void concurrentUpdates() throws Exception {
        concurrentUpdates(false);
    }

    private void concurrentUpdates(final boolean useBranch) throws Exception {
        LOG.info("====== Start test =======");
        final AtomicInteger conflicts = new AtomicInteger();
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        List<Thread> writers = new ArrayList<Thread>();
        for (final MicroKernel mk : kernels) {
            writers.add(new Thread(new Runnable() {
                Random random = new Random();
                Map<Integer, JSONObject> nodes = new HashMap<Integer, JSONObject>();
                @Override
                public void run() {
                    BitSet conflictSet = new BitSet();
                    int numTransfers = NUM_TRANSFERS_PER_THREAD;
                    try {
                        while (numTransfers > 0) {
                            try {
                                if (!transfer()) {
                                    continue;
                                }
                            } catch (MicroKernelException e) {
                                log("Failed transfer @" + mk.getHeadRevision());
                                // assume conflict
                                conflicts.incrementAndGet();
                                conflictSet.set(numTransfers);
                            }
                            numTransfers--;
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                    log("conflicts: " + conflictSet);
                }

                private boolean transfer() throws Exception {
                    // read 3 random nodes and re-distribute values
                    nodes.clear();
                    while (nodes.size() < 3) {
                        nodes.put(random.nextInt(NUM_NODES), null);
                    }
                    String rev;
                    if (useBranch) {
                        rev = mk.branch(null);
                    } else {
                        rev = mk.getHeadRevision();
                    }
                    int sum = 0;
                    for (Map.Entry<Integer, JSONObject> entry : nodes.entrySet()) {
                        String json = mk.getNodes("/node-" + entry.getKey(), rev, 0, 0, 1000, null);
                        JSONParser parser = new JSONParser();
                        JSONObject obj = (JSONObject) parser.parse(json);
                        entry.setValue(obj);
                        sum += (Long) obj.get("value");
                    }
                    if (sum < 60) {
                        // retry with other nodes
                        return false;
                    }
                    StringBuilder jsop = new StringBuilder();
                    boolean withdrawn = false;
                    for (Map.Entry<Integer, JSONObject> entry : nodes.entrySet()) {
                        long value = (Long) entry.getValue().get("value");
                        jsop.append("^\"/node-").append(entry.getKey());
                        jsop.append("/value\":");
                        if (value >= 20 && !withdrawn) {
                            jsop.append(value - 20);
                            withdrawn = true;
                        } else {
                            jsop.append(value + 10);
                        }
                    }
                    String oldRev = rev;
                    rev = mk.commit("", jsop.toString(), rev, null);
                    if (useBranch) {
                        rev = mk.merge(rev, null);
                    }
                    log("Successful transfer @" + oldRev + ": " + jsop.toString() + " (new rev: " + rev + ")");
                    return true;
                }
            }));
        }
        for (Thread t : writers) {
            t.start();
        }
        for (Thread t : writers) {
            t.join();
        }
        // dispose will flush all pending revisions
        for (MongoMK mk : kernels) {
            mk.dispose();
        }
        MongoMK mk = openMongoMK();
        String rev = mk.getHeadRevision();
        long sum = 0;
        for (int i = 0; i < NUM_NODES; i++) {
            String json = mk.getNodes("/node-" + i, rev, 0, 0, 1000, null);
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject) parser.parse(json);
            sum += (Long) obj.get("value");
        }
        log("Conflict rate: " + conflicts.get() +
                "/" + (NUM_WRITERS * NUM_TRANSFERS_PER_THREAD));
        System.out.print(logBuffer);
        assertEquals(NUM_NODES * 100, sum);
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }

    void log(String msg) {
        if (USE_LOGGER) {
            LOG.info(msg);
        } else {
            synchronized (logBuffer) {
                logBuffer.append(msg).append("\n");
            }
        }
    }
}
