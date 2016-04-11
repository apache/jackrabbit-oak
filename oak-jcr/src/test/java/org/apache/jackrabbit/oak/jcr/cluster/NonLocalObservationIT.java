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
package org.apache.jackrabbit.oak.jcr.cluster;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.fixture.DocumentMongoFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

/**
 * Test for external events from another cluster node.
 */
public class NonLocalObservationIT extends AbstractClusterTest {

    private static final Logger log = LoggerFactory.getLogger(NonLocalObservationIT.class);

    AtomicReference<Exception> exception = new AtomicReference<Exception>();

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(FixturesHelper.getFixtures().contains(DOCUMENT_NS) && MongoUtils.isAvailable());
    }

    @Override
    protected NodeStoreFixture getFixture() {
        /**
         * Fixes the cluster use case plus allowing to control the cache sizes.
         * In theory other users of DocumentMongoFixture might have similar 
         * test cases - but keeping it simple for now - thus going via subclass.
         */
        return new DocumentMongoFixture() {
            
            private String clusterSuffix = System.currentTimeMillis() + "-NonLocalObservationIT";

            private DB db;
            
            /** keep a reference to the node stores so that the db only gets closed after the last nodeStore was closed */
            private Set<NodeStore> nodeStores = new HashSet<NodeStore>();

            /**
             * This is not implemented in the super class at all.
             * <ul>
             *  <li>use a specific suffix to make sure we have our own, new db and clean it up after the test</li>
             *  <li>properly drop that db created above in dispose</li>
             *  <li>use only 32MB (vs default of 256MB) memory to ensure we're not going OOM just because of this (which happens with the default)</li>
             *  <li>disable the persistent cache for the same reason</li>
             * </ul>
             */
            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                try {
                    DocumentMK.Builder builder = new DocumentMK.Builder();
                    builder.memoryCacheSize(32*1024*1024); // keep this one low to avoid OOME
                    builder.setPersistentCache(null);      // turn this one off to avoid OOME
                    final String suffix = clusterSuffix;
                    db = getDb(suffix); // db will be overwritten - but that's fine
                    builder.setMongoDB(db);
                    DocumentNodeStore ns = builder.getNodeStore();
                    nodeStores.add(ns);
                    return ns;
                } catch (Exception e) {
                    throw new AssumptionViolatedException("Mongo instance is not available", e);
                }
            }
            
            @Override
            public void dispose(NodeStore nodeStore) {
                super.dispose(nodeStore);
                nodeStores.remove(nodeStore);
                if (db != null && nodeStores.size() == 0) {
                    try {
                        db.dropDatabase();
                        db.getMongo().close();
                        db = null;
                    } catch (Exception e) {
                        log.error("dispose: Can't close Mongo", e);
                    }
                }
            }
            
            @Override
            public String toString() {
                return "NonLocalObservationIT's DocumentMongoFixture flavour";
            }

        };
    }

    @Override
    protected void prepareTestData(Session s) throws RepositoryException {
        if (s.itemExists("/test")) {
            s.getNode("/test").remove();
            s.save();
        }
        s.getRootNode().addNode("test", "oak:Unstructured");
        s.save();
    }

    private void addEventHandler(Session s, final String expectedNodeSuffix) throws Exception {
        ObservationManager o = s.getWorkspace().getObservationManager();
        o.addEventListener(new EventListener() {
            @Override
            public void onEvent(EventIterator events) {
                while (events.hasNext()) {
                    Event e = events.nextEvent();
                    if (!(e instanceof JackrabbitEvent)) {
                        continue;
                    }
                    if (((JackrabbitEvent) e).isExternal()) {
                        continue;
                    }
                    String p;
                    try {
                        p = e.getPath();
                        // log.info("expectedNodeSuffix:
                        // "+expectedNodeSuffix+", path: " + p);
                        if (!p.endsWith(expectedNodeSuffix)) {
                            log.info("EXCEPTION: expectedNodeSuffix: " + expectedNodeSuffix + ", path: " + p);
                            throw new Exception("expectedNodeSuffix: " + expectedNodeSuffix + ", non-local path: " + p);
                        }
                    } catch (Exception e1) {
                        exception.set(e1);
                    }
                }
            }
        }, Event.NODE_ADDED, "/", true, null, null, false);
    }

    @Test
    public void randomized() throws Exception {
        log.info(new Date() + ": initialization");
        if (s1 == null) {
            return;
        }
        addEventHandler(s1, "1");
        addEventHandler(s2, "2");
        Random r = new Random(1);
        // phase 1 is measuring how long 10000 iterations take 
        // (is taking 4-6sec on my laptop)
        log.info(new Date() + ": measuring 10000 iterations...");
        long scaleMeasurement = doRandomized(r, 10000);
        // phase 2 is 10 times measuring how long subsequent 10000 iterations take
        //  (this used to fail due to 'many commit roots')
        boolean ignoreFirstSpike = true;
        for (int i = 0; i < 14; i++) {
            log.info(new Date() + ": test run of 10000 iterations...");
            long testMeasurement = doRandomized(r, 10000);
            Exception e = exception.get();
            if (e != null) {
                throw e;
            }
            // the testMeasurement should now take less than 200% in relation to
            // the
            // scaleMeasurement
            long max = (long) (scaleMeasurement * 3);
            log.info(new Date() + ": test run took " + testMeasurement + ", scaleMeasurement=" + scaleMeasurement
                    + ", plus 200% margin: " + max);
            if (testMeasurement >= max && ignoreFirstSpike) {
                log.info(new Date() + ": this iteration would have failed, but we're now allowing one spike (ignoreFirstSpike)");
                ignoreFirstSpike = false;
                continue;
            }
            assertTrue("test run (" + testMeasurement + ") took more than 200% longer than initial measurement (" + scaleMeasurement
                    + ") (check VM memory settings)", testMeasurement < max);
        }

    }

    private long doRandomized(Random r, long loopCnt) throws Exception {
        final long start = System.currentTimeMillis();
        long lastOut = System.currentTimeMillis();
        for (int i = 0; i < loopCnt && exception.get() == null; i++) {
            if (i % 1000 == 0) {
                long now = System.currentTimeMillis();
                long diff = now - lastOut;
                lastOut = now;
                log.info(new Date() + ": diff: " + diff + " for " + i + "/" + 100000);
            }
            int sId = r.nextBoolean() ? 1 : 2;
            Session s = sId == 1 ? s1 : s2;
            Node test = s.getRootNode().getNode("test");
            String nodeName = "n" + r.nextInt(10000) + sId;
            switch (r.nextInt(3)) {
                case 0:
                    try {
                        s.save();
                    } catch (RepositoryException e) {
                        s.refresh(false);
                    }
                    break;
                case 1:
                    if (!test.hasNode(nodeName)) {
                        test.addNode(nodeName, "oak:Unstructured");
                    }
                    break;
                case 2:
                    if (test.hasNode(nodeName)) {
                        test.getNode(nodeName).remove();
                    }
                    break;
                case 3:
                    if (test.hasNode(nodeName)) {
                        test.getNode(nodeName).setProperty("test", r.nextInt(10));
                    }
                    break;
            }
        }
        return System.currentTimeMillis() - start;
    }

}
