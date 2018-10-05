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
package org.apache.jackrabbit.oak.jcr.observation;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.test.api.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ObservationRefreshTest extends AbstractRepositoryTest {
    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final String TEST_NODE = "test_node";
    private static final String TEST_NODE_TYPE = "oak:Unstructured";
    private static final String REFERENCEABLE_NODE = "\"referenceable\"";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final String TEST_TYPE = "mix:test";

    private static final long CONDITION_TIMEOUT = 10*60*1000;

    private Session observingSession;
    private ObservationManager observationManager;

    public ObservationRefreshTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Override
    protected Jcr initJcr(Jcr jcr) {
        // Ensure the observation revision queue is sufficiently large to hold
        // all revisions. Otherwise waiting for events might block since pending
        // events would only be released on a subsequent commit. See OAK-1491
        return jcr.withObservationQueueLength(1000000);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();

        NodeTypeManager ntMgr = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName(TEST_TYPE);
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node n = session.getRootNode().addNode(TEST_NODE, TEST_NODE_TYPE);
        n.addMixin(TEST_TYPE);
        Node refNode = n.addNode(REFERENCEABLE_NODE);
        refNode.addMixin(JcrConstants.MIX_REFERENCEABLE);

        session.save();

        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put(RepositoryImpl.REFRESH_INTERVAL, 0);
        observingSession = ((JackrabbitRepository) getRepository()).login(new SimpleCredentials("admin", "admin".toCharArray()), null, attrs);
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        observingSession.logout();
    }

    @Test
    public void observation() throws RepositoryException, InterruptedException, ExecutionException {
        final MyListener listener = new MyListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getAdminSession().getNode(TEST_PATH);
            for (int i=0; i<1000; i++) {
                n.addNode("n" + i);
                n.getSession().save();
            }
            listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                @Override
                public boolean evaluate() {
                    return listener.numAdded == 1000;
                }
            });
            assertEquals("", listener.error);
            assertEquals("added nodes", 1000, listener.numAdded);

            for (int i=0; i<1000; i++) {
                n.getNode("n" + i).remove();
                n.getSession().save();
            }
            listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                @Override
                public boolean evaluate() {
                    return listener.numRemoved == 1000;
                }
            });
            assertEquals("", listener.error);
            assertEquals("removed nodes", 1000, listener.numRemoved);

            for (int i=0; i<100; i++) {
                n.setProperty("test" + i, "foo");
                n.getSession().save();
            }
            listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                @Override
                public boolean evaluate() {
                    return listener.numPropsAdded == 1100;
                }
            });
            assertEquals("", listener.error);
            assertEquals("properties added", 1100, listener.numPropsAdded);

            for (int i=0; i<100; i++) {
                n.setProperty("test" + i, i);
                n.getSession().save();
            }
            listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                @Override
                public boolean evaluate() {
                    return listener.numPropsModified == 100;
                }
            });
            assertEquals("", listener.error);
            assertEquals("properties modified", 100, listener.numPropsModified);

            for (int i=0; i<10; i++) {
                n.setProperty("test100", "foo");
                n.getSession().save();
                assertTrue("Gave up waiting for events",
                        listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                            @Override
                            public boolean evaluate() {
                                return listener.test100Exists;
                            }
                        }));
                n.getProperty("test100").remove();
                n.getSession().save();
                assertTrue("Gave up waiting for events",
                        listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                            @Override
                            public boolean evaluate() {
                                return !listener.test100Exists;
                            }
                        }));
            }
            assertEquals("", listener.error);

            for (int i=0; i<100; i++) {
                n.getProperty("test" + i).remove();
                n.getSession().save();
            }
            listener.waitFor(CONDITION_TIMEOUT, new Condition() {
                @Override
                public boolean evaluate() {
                    return listener.numPropsRemoved == 1100;
                }
            });
            assertEquals("", listener.error);
            assertEquals("properties removed", 1100, listener.numPropsRemoved);
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    private interface Condition {
        boolean evaluate();
    }

    private class MyListener implements EventListener {

        private volatile String error = "";

        private volatile int numAdded = 0;

        private volatile int numRemoved = 0;

        private volatile int numPropsAdded = 0;

        private volatile int numPropsRemoved = 0;

        private volatile int numPropsModified = 0;

        private volatile boolean test100Exists = false;

        @Override
        public synchronized void onEvent(EventIterator events) {
            try {
                while (events.hasNext()) {
                    Event event = events.nextEvent();
                    if (event.getPath().startsWith("/oak:index")) {
                        continue;
                    }

                    if (event.getType() == Event.NODE_ADDED) {
                        numAdded++;
                        if (!observingSession.nodeExists(event.getPath())) {
                            error = "node missing: " + event.getPath();
                        }
                    }
                    if (event.getType() == Event.NODE_REMOVED) {
                        numRemoved++;
                        if (observingSession.nodeExists(event.getPath())) {
                            error = "node not missing: " + event.getPath();
                        }
                    }
                    if (event.getType() == Event.PROPERTY_ADDED) {
                        Node node = observingSession.getNode(Text.getRelativeParent(event.getPath(), 1));
                        PropertyIterator iter = node.getProperties();
                        boolean ok = false;
                        while (iter.hasNext()) {
                            Property p = iter.nextProperty();
                            if (p.getPath().equals(event.getPath())) {
                                ok = true;
                            }
                        }
                        if (!ok) {
                            error = "property missing: " + event.getPath();
                        }
                        String name = Text.getName(event.getPath());
                        if ("test100".equals(name)) {
                            test100Exists = true;
                        } else {
                            numPropsAdded++;
                            if (!observingSession.propertyExists(event.getPath())) {
                                error = "property missing: " + event.getPath();
                            }
                        }
                    }
                    if (event.getType() == Event.PROPERTY_REMOVED) {
                        String name = Text.getName(event.getPath());
                        if ("test100".equals(name)) {
                            test100Exists = false;
                        } else {
                            numPropsRemoved++;
                            if (observingSession.propertyExists(event.getPath())) {
                                error = "property not missing: " + event.getPath();
                            }
                        }
                    }
                    if (event.getType() == Event.PROPERTY_CHANGED) {
                        String name = Text.getName(event.getPath());
                        if ("test100".equals(name)) {

                        } else {
                            numPropsModified++;
                            long v = observingSession.getProperty(event.getPath()).getLong();
                            if (v != Long.valueOf(Text.getName(name).substring(4))) {
                                error = "property has wrong content: " + event.getPath();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                error = e.toString();
                e.printStackTrace();
            } finally {
                notifyAll();
            }
        }

        synchronized boolean waitFor(long timeout, Condition c)
                throws InterruptedException {
            long end = System.currentTimeMillis() + timeout;
            long remaining = end - System.currentTimeMillis();
            while (remaining > 0) {
                if (c.evaluate()) {
                    return true;
                }
                wait(remaining);
                remaining = end - System.currentTimeMillis();
            }
            return false;
        }

    }
}
