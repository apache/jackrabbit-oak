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

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Updates multiple nodes in the same commit with multiple threads and verifies
 * the commit is atomic.
 */
public class ConcurrentConflictTest extends BaseDocumentMKTest {

    private static final boolean USE_LOGGER = true;
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentConflictTest.class);
    private static final int NUM_WRITERS = 3;
    private static final int NUM_NODES = 10;
    private static final int NUM_TRANSFERS_PER_THREAD = 100;
    private DocumentStore store;
    private List<DocumentMK> kernels = new ArrayList<DocumentMK>();
    private final StringBuilder logBuffer = new StringBuilder();

    @Before
    @Override
    public void initDocumentMK() {
        logBuffer.setLength(0);
        this.store = new MemoryDocumentStore();
        DocumentMK mk = openDocumentMK(1);
        for (int i = 0; i < NUM_NODES; i++) {
            mk.commit("/", "+\"node-" + i + "\":{\"value\":100}", null, null);
        }
        mk.dispose();
        for (int i = 0; i < NUM_WRITERS; i++) {
            kernels.add(openDocumentMK(i + 2));
        }
    }

    @After
    @Override
    public void disposeDocumentMK() {
        super.disposeDocumentMK();
        for (DocumentMK mk : kernels) {
            mk.dispose();
        }
        kernels.clear();
    }

    private DocumentMK openDocumentMK(int clusterId) {
        return new DocumentMK.Builder().setAsyncDelay(10).setDocumentStore(store).setClusterId(clusterId).open();
    }

    @Test
    public void concurrentUpdatesWithBranch() throws Exception {
        concurrentUpdates(true);
    }

    @Test
    public void concurrentUpdates() throws Exception {
        concurrentUpdates(false);
    }

    @Ignore("Enable to run concurrentUpdates() in a loop")
    @Test
    public void concurrentUpdates_Loop() throws Exception {
        for (int i = 0; i < 1000; i++) {
            System.out.println("test " + i);
            concurrentUpdates(false);
            // prepare for next round
            disposeDocumentMK();
            initDocumentMK();
        }
    }

    private void concurrentUpdates(final boolean useBranch) throws Exception {
        LOG.info("====== Start test =======");
        final AtomicInteger conflicts = new AtomicInteger();
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        List<Thread> writers = new ArrayList<Thread>();
        for (final DocumentMK mk : kernels) {
            writers.add(new Thread(new Runnable() {
                Random random = new Random();
                Map<Integer, JSONObject> nodes = new HashMap<Integer, JSONObject>();
                @Override
                public void run() {
                    BitSet conflictSet = new BitSet();
                    int numTransfers = 0;
                    try {
                        while (numTransfers < NUM_TRANSFERS_PER_THREAD && exceptions.isEmpty()) {
                            try {
                                if (!transfer()) {
                                    continue;
                                }
                            } catch (DocumentStoreException e) {
                                log("Failed transfer @" + mk.getHeadRevision());
                                // assume conflict
                                conflicts.incrementAndGet();
                                conflictSet.set(numTransfers);
                            }
                            numTransfers++;
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                    log("conflicts (" + conflictSet.cardinality() + "): " + conflictSet);
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
                    long s = calculateSum(mk, rev);
                    if (s != NUM_NODES * 100) {
                        throw new Exception("Sum mismatch: " + s);
                    }
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
        for (DocumentMK mk : kernels) {
            mk.dispose();
        }
        DocumentMK mk = openDocumentMK(1);
        String rev = mk.getHeadRevision();
        long sum = calculateSum(mk, rev);
        log("Conflict rate: " + conflicts.get() +
                "/" + (NUM_WRITERS * NUM_TRANSFERS_PER_THREAD));
        System.out.print(logBuffer);
        assertEquals(NUM_NODES * 100, sum);
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        mk.dispose();
    }

    static long calculateSum(DocumentMK mk, String rev) throws Exception {
        long sum = 0;
        for (int i = 0; i < NUM_NODES; i++) {
            String json = mk.getNodes("/node-" + i, rev, 0, 0, 1000, null);
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject) parser.parse(json);
            sum += (Long) obj.get("value");
        }
        return sum;
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
