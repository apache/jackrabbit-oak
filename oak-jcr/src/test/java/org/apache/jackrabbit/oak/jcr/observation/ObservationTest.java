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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObservationTest extends AbstractRepositoryTest {
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private Session observingSession;
    private ObservationManager observationManager;

    @Before
    public void setup() throws RepositoryException {
        executor = Executors.newScheduledThreadPool(1);
        Session session = getAdminSession();
        session.getRootNode().addNode(TEST_NODE);
        session.save();

        observingSession = createAdminSession();
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        observingSession.logout();
    }

    @Test
    public void observation() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(
                listener, NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED | PROPERTY_REMOVED |
                PROPERTY_CHANGED | PERSIST, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.setProperty("p0", "v0"));
            Node n1 = listener.expectAdd(n.addNode("n1"));
            listener.expectAdd(n1.setProperty("p1", "v1"));
            listener.expectAdd(n1.setProperty("p2", "v2"));
            listener.expectAdd(n.addNode("n2"));
            getAdminSession().save();

            List<String> missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.setProperty("property", 42));
            Node n3 = listener.expectAdd(n.addNode("n3"));
            listener.expectAdd(n3.setProperty("p3", "v3"));
            listener.expectChange(n1.setProperty("p1", "v1.1"));
            listener.expectRemove(n1.getProperty("p2")).remove();
            listener.expectRemove(n.getNode("n2")).remove();
            listener.expectAdd(n.addNode("{4}"));
            getAdminSession().save();

            missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observation2() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(
                listener, NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED | PROPERTY_REMOVED |
                PROPERTY_CHANGED | PERSIST, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.addNode("n1"));
            getAdminSession().save();

            List<String> missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.addNode("n2"));
            listener.expectRemove(n.getNode("n1")).remove();
            getAdminSession().save();

            missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observationOnRootNode() throws Exception {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, PROPERTY_ADDED, "/", true, null, null, false);
        try {
            // add property to root node
            Node root = getNode("/");
            listener.expectAdd(root.setProperty("prop", "value"));
            root.getSession().save();

            List<String> missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void observationDispose()
            throws RepositoryException, InterruptedException, ExecutionException, TimeoutException {

        final AtomicReference<Boolean> stopGeneratingEvents = new AtomicReference<Boolean>(false);
        final AtomicReference<CountDownLatch> hasEvents = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        try {
            final EventListener listener = new EventListener() {
                @Override
                public void onEvent(EventIterator events) {
                    while (events.hasNext()) {
                        events.next();
                        hasEvents.get().countDown();
                    }
                }
            };

            observationManager.addEventListener(
                    listener, NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED | PROPERTY_REMOVED |
                    PROPERTY_CHANGED | PERSIST, "/", true, null, null, false);

            // Generate events
            Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Node n = getNode(TEST_PATH);
                    for (int c = 0; !stopGeneratingEvents.get() ; c++) {
                        n.addNode("c" + c);
                        n.getSession().save();
                    }
                    return null;
                }
            });

            // Make sure we see the events
            assertTrue(hasEvents.get().await(2, TimeUnit.SECONDS));

            // Remove event listener
            Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    observationManager.removeEventListener(listener);
                    hasEvents.set(new CountDownLatch(1));
                    return null;
                }
            }).get(10, TimeUnit.SECONDS);

            // Make sure we don't see any more events
            assertFalse(hasEvents.get().await(2, TimeUnit.SECONDS));
        }
        finally {
            stopGeneratingEvents.set(true);
        }
    }

    @Test
    public void observationDisposeFromListener()
            throws RepositoryException, InterruptedException, ExecutionException, TimeoutException {

        final AtomicReference<RepositoryException> repositoryException = new AtomicReference<RepositoryException>(null);
        final AtomicReference<CountDownLatch> unregistered = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final EventListener listener = new EventListener() {
            @Override
            public void onEvent(EventIterator events) {
                try {
                    // Unregistering listener from event handler should not block
                    observationManager.removeEventListener(this);
                }
                catch (RepositoryException e) {
                    repositoryException.set(e);
                }
                finally {
                    unregistered.get().countDown();
                }
            }
        };

        observationManager.addEventListener(
                listener, NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED | PROPERTY_REMOVED |
                PROPERTY_CHANGED | PERSIST, "/", true, null, null, false);

        // Ensure the listener is there
        assertTrue(observationManager.getRegisteredEventListeners().hasNext());

        // Generate events
        Node n = getNode(TEST_PATH);
        n.addNode("c");
        n.getSession().save();

        // Make sure we see the events and the listener is gone
        assertTrue(unregistered.get().await(2, TimeUnit.SECONDS));
        if (repositoryException.get() != null) {
            throw repositoryException.get();
        }
        assertFalse(observationManager.getRegisteredEventListeners().hasNext());
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }

    //------------------------------------------------------------< ExpectationListener >---

    private static class ExpectationListener implements EventListener {
        private final Map<String, SettableFuture<Event>> expected = Maps.newConcurrentMap();
        private final List<Event> unexpected = Lists.newCopyOnWriteArrayList();
        private volatile Exception failed;

        public Future<Event> expect(String path, int type) {
            if (failed == null) {
                SettableFuture<Event> expect = SettableFuture.create();
                expected.put(key(path, type), expect);
                return expect;
            } else {
                return Futures.immediateFailedFuture(failed);
            }
        }

        public Node expectAdd(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_ADDED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
            return node;
        }

        public Node expectRemove(Node node) throws RepositoryException {
            expect(node.getPath(), NODE_REMOVED);
            expect(node.getPath() + "/jcr:primaryType", PROPERTY_REMOVED);
            return node;
        }

        public Property expectAdd(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_ADDED);
            return property;
        }

        public Property expectRemove(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_REMOVED);
            return property;
        }

        public Property expectChange(Property property) throws RepositoryException {
            expect(property.getPath(), PROPERTY_CHANGED);
            return property;
        }

        public List<String> getMissing(int time, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException {
            List<String> missing = Lists.newArrayList();
            try {
                Futures.allAsList(expected.values()).get(time, timeUnit);
            }
            catch (TimeoutException e) {
                for (Entry<String, SettableFuture<Event>> entry : expected.entrySet()) {
                    if (!entry.getValue().isDone()) {
                        missing.add(entry.getKey());
                    }
                }
            }
            return missing;
        }

        public List<Event> getUnexpected() {
            return Lists.newArrayList(unexpected);
        }

        @Override
        public void onEvent(EventIterator events) {
            try {
                while (events.hasNext() && failed == null) {
                    Event event = events.nextEvent();
                    SettableFuture<Event> f = expected.get(key(event.getPath(), event.getType()));
                    if (f != null) {
                        f.set(event);
                    } else {
                        unexpected.add(event);
                    }
                }
            } catch (Exception e) {
                for (SettableFuture<Event> f : expected.values()) {
                    f.setException(e);
                }
                failed = e;
            }
        }

        private static String key(String path, int type) {
            return path + ':' + type;
        }
    }

}
