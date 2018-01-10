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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.TestUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

/**
 * A long running resilience IT. The test sets up a three node replica set and
 * adds nodes in batches between one and ten nodes. In the background a task
 * periodically stops the MongoDB primary for 30 seconds. A reader thread
 * verifies all nodes are present. This test is skipped by default and can be
 * enabled with a system property {@code -Dtest=ReplicaSetResilienceIT}.
 */
public class ReplicaSetResilienceIT {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetResilienceIT.class);

    private static final int NUM_NODES = Integer.getInteger(ReplicaSetResilienceIT.class.getSimpleName() + ".numNodes", 100 * 1000);

    @Rule
    public MongodProcessFactory mongodProcessFactory = new MongodProcessFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Map<Integer, MongodProcess> executables = new HashMap<>();

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private Random random = new Random();

    private DocumentNodeStore ns;

    private List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

    @BeforeClass
    public static void checkEnabled() {
        assumeThat(ReplicaSetResilienceIT.class.getSimpleName(), is(System.getProperty("test")));
    }

    @Before
    public void before() throws IOException {
        executables.putAll(mongodProcessFactory.startReplicaSet("rs", 3));
        // crash and restart the primary once a minute
        executorService.scheduleWithFixedDelay(new PrimaryCrasher(),
                30, 30, TimeUnit.SECONDS);
        String uri = "mongodb://" + MongodProcessFactory.localhost(executables.keySet());
        ns = builderProvider.newBuilder().setMongoDB(uri, MongoUtils.DB, 0).build();
    }

    @Test
    public void start() throws Exception {
        Thread reader = new Thread(new Verifier(), "Reader");
        reader.start();
        AtomicInteger i = new AtomicInteger();
        while (i.get() < NUM_NODES && exceptions.isEmpty()) {
            NodeBuilder builder = ns.getRoot().builder();
            Iterable<String> names = addNodes(builder, i);
            TestUtils.merge(ns, builder);
            LOG.info("Created {}", names);
        }
        reader.join();
        for (Exception e : exceptions) {
            throw e;
        }
        verifyAll();
    }

    private void verifyAll() {
        Stopwatch sw = Stopwatch.createStarted();
        Iterator<String> names = ns.getRoot().getChildNodeNames().iterator();
        for (int i = 0; i < NUM_NODES; i++) {
            assertTrue(names.hasNext());
            String name = nodeName(i);
            assertEquals(name, names.next());
            LOG.info("Verified {}", name);
        }
        long rate = NUM_NODES * 1000L;
        rate = rate / sw.elapsed(TimeUnit.MILLISECONDS);
        LOG.info("Verified at {} nodes/s", rate);
    }

    private Iterable<String> addNodes(NodeBuilder builder, AtomicInteger counter) {
        List<String> names = new ArrayList<>();
        // create between one and ten nodes
        int numNodes = random.nextInt(10) + 1;
        for (int i = 0; i < numNodes; i++) {
            String name = nodeName(counter.getAndIncrement());
            builder.child(name);
            names.add(name);
        }
        return names;
    }

    private static String nodeName(int i) {
        return String.format("node-%09d", i);
    }

    private class Verifier implements Runnable {

        @Override
        public void run() {
            try {
                for (int i = 0; i < NUM_NODES; i++) {
                    await(i);
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        private void await(int i) {
            String name = nodeName(i);
            NodeState root = ns.getRoot();
            while (!root.hasChildNode(name)) {
                try {
                    // sleep a bit
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // ignore
                }
                // must not see a gap (look ahead 3 nodes)
                for (int j = 1; j <= 3; j++) {
                    assertFalse(root.hasChildNode(nodeName(i + j)));
                }
                // get a fresh root
                root = ns.getRoot();
            }
            LOG.info("Seen {}", name);
        }
    }

    private class PrimaryCrasher implements Runnable {

        private volatile int stopped;

        @Override
        public void run() {
            try {
                try {
                    if (stopped > 0) {
                        start(stopped);
                    } else {
                        stopPrimary();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } catch (Exception e) {
                LOG.warn("Exception running task", e);
                throw e;
            }
        }

        private void start(int port) throws IOException {
            LOG.info("=== Starting MongoDB on port {}", port);
            executables.get(port).start();
            stopped = 0;
        }

        private void stopPrimary() {
            List<ServerAddress> seeds = new ArrayList<>();
            for (MongodProcess p : executables.values()) {
                seeds.add(p.getAddress());
            }
            try (MongoClient c = new MongoClient(seeds,
                    new MongoClientOptions.Builder().requiredReplicaSetName("rs").build())) {
                ServerAddress address = null;
                for (int i = 0; i < 5; i++) {
                    address = c.getReplicaSetStatus().getMaster();
                    if (address == null) {
                        LOG.info("Primary unavailable. Waiting one second...");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    } else {
                        break;
                    }
                }
                if (address == null) {
                    LOG.warn("=== ReplicaSet does not (yet?) have a primary");
                } else {
                    try {
                        LOG.info("=== Stopping MongoDB on port {}", address.getPort());
                        MongodProcess proc = executables.get(address.getPort());
                        for (int i = 0; i < 5; i++) {
                            try {
                                proc.stop();
                                stopped = address.getPort();
                                break;
                            } catch (Exception e) {
                                LOG.warn("Stopping mongod process failed ({}/5): {}", i + 1, e);
                            }
                        }
                        if (stopped != 0) {
                            LOG.info("=== Stopped primary on port {}", stopped);
                        } else {
                            LOG.info("=== Unable to stop primary on port {}", address.getPort());
                        }
                    } catch (Exception e) {
                        LOG.error("Exception stopping primary", e);
                    }
                }
            }
        }
    }
}
