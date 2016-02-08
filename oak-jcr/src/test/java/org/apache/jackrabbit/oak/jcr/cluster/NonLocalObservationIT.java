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

import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

/**
 * Test for external events from another cluster node.
 */
public class NonLocalObservationIT extends AbstractClusterTest {

    AtomicReference<Exception> exception = new AtomicReference<Exception>();

    @Override
    protected NodeStoreFixture getFixture() {
        return new NodeStoreFixture() {

            private DocumentStore documentStore;

            private DocumentStore getDocumentStore() {
                if (documentStore == null) {
                    documentStore = new MemoryDocumentStore();
                }
                return documentStore;
            }

            @Override
            public String toString() {
                return "TestNodeStoreFixture";
            }

            @Override
            public NodeStore createNodeStore() {
                return new DocumentMK.Builder().setDocumentStore(getDocumentStore()).getNodeStore();
            }

            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                return new DocumentMK.Builder().setDocumentStore(getDocumentStore()).setClusterId(clusterNodeId).getNodeStore();
            }
        };
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
                        // System.out.println("expectedNodeSuffix:
                        // "+expectedNodeSuffix+", path: " + p);
                        if (!p.endsWith(expectedNodeSuffix)) {
                            System.out.println("EXCEPTION: expectedNodeSuffix: " + expectedNodeSuffix + ", path: " + p);
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
        System.out.println(new Date() + ": initialization");
        if (s1 == null) {
            return;
        }
        if (s1.itemExists("/test")) {
            s1.getNode("/test").remove();
            s1.save();
        }
        s1.getRootNode().addNode("test", "oak:Unstructured");
        s1.save();
        addEventHandler(s1, "1");
        addEventHandler(s2, "2");
        Thread.sleep(2000);
        Random r = new Random(1);
        // phase 1 is measuring how long 10000 iterations take 
        // (is taking 4-6sec on my laptop)
        System.out.println(new Date() + ": measuring 10000 iterations...");
        long scaleMeasurement = doRandomized(r, 10000);
        // phase 2 is 10 times measuring how long subsequent 10000 iterations take
        //  (this used to fail due to 'many commit roots')
        boolean ignoreFirstSpike = true;
        for (int i = 0; i < 15; i++) {
            System.out.println(new Date() + ": test run of 10000 iterations...");
            long testMeasurement = doRandomized(r, 10000);
            Exception e = exception.get();
            if (e != null) {
                throw e;
            }
            // the testMeasurement should now take less than 50% in relation to
            // the
            // scaleMeasurement
            long max = (long) (scaleMeasurement * 1.5);
            System.out.println(new Date() + ": test run took " + testMeasurement + ", scaleMeasurement=" + scaleMeasurement
                    + ", plus 50% margin: " + max);
            if (testMeasurement >= max && ignoreFirstSpike) {
                System.out.println(new Date() + ": this iteration would have failed, but we're now allowing one spike (ignoreFirstSpike)");
                ignoreFirstSpike = false;
                continue;
            }
            assertTrue("test run (" + testMeasurement + ") took more than 50% longer than initial measurement (" + scaleMeasurement
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
                System.out.println(new Date() + ": diff: " + diff + " for " + i + "/" + 100000);
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
