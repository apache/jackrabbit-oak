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

import static com.google.common.base.Objects.equal;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObservationTest extends AbstractRepositoryTest {
    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private Session observingSession;
    private ObservationManager observationManager;

    @Before
    public void setup() throws RepositoryException {
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
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.setProperty("p0", "v0"));
            Node n1 = listener.expectAdd(n.addNode("n1"));
            listener.expectAdd(n1.setProperty("p1", "v1"));
            listener.expectAdd(n1.setProperty("p2", "v2"));
            listener.expectAdd(n.addNode("n2"));
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(2, TimeUnit.SECONDS);
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
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expectAdd(n.addNode("n1"));
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(2, TimeUnit.SECONDS);
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

            List<Expectation> missing = listener.getMissing(2, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void pathFilter() throws Exception {
        final String path = "/events/only/here";
        ExpectationListener listener = new ExpectationListener();
        listener.expect(new Expectation(path){
            @Override
            public boolean onEvent(Event event) throws Exception {
                return PathUtils.isAncestor(path, event.getPath());
            }
        });

        observationManager.addEventListener(listener, NODE_ADDED, path, true, null, null, false);
        try {
            Node root = getNode("/");
            root.addNode("events").addNode("only").addNode("here").addNode("at");
            root.getSession().save();

            List<Expectation> missing = listener.getMissing(2, TimeUnit.SECONDS);
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

        final ExpectationListener listener = new ExpectationListener();
        Expectation hasEvents = listener.expect(
                new Expectation("has events after registering"));
        final Expectation noEvents = listener.expect(
                new Expectation("has no more events after unregistering", false));

        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        // Generate events
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            private int c;

            @Override
            public void run() {
                try {
                    getNode(TEST_PATH)
                            .addNode("c" + c++)
                            .getSession()
                            .save();
                }
                catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10, 10, TimeUnit.MILLISECONDS);

        // Make sure we see the events
        assertNotNull(hasEvents.get(2, TimeUnit.SECONDS));

        // Remove event listener
        Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                observationManager.removeEventListener(listener);
                noEvents.enable(true);
                return null;
            }
        }).get(10, TimeUnit.SECONDS);

        // Make sure we see no more events
        assertFalse(noEvents.wait(2, TimeUnit.SECONDS));
    }

    @Test
    public void observationDisposeFromListener()
            throws RepositoryException, InterruptedException, ExecutionException, TimeoutException {

        final ExpectationListener listener = new ExpectationListener();
        Expectation unregistered = listener.expect(new Expectation
                ("Unregistering listener from event handler should not block") {
            @Override
            public boolean onEvent(Event event) throws Exception {
                observationManager.removeEventListener(listener);
                return true;
            }
        });

        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        // Ensure the listener is there
        assertTrue(observationManager.getRegisteredEventListeners().hasNext());

        // Generate events
        Node n = getNode(TEST_PATH);
        n.addNode("c");
        n.getSession().save();

        // Make sure we see the events and the listener is gone
        assertNotNull(unregistered.get(2, TimeUnit.SECONDS));
        assertFalse(observationManager.getRegisteredEventListeners().hasNext());
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }

    //------------------------------------------------------------< ExpectationListener >---

    private static class Expectation extends ForwardingListenableFuture<Event> {
        private final SettableFuture<Event> future = SettableFuture.create();
        private final String name;

        private volatile boolean enabled = true;

        Expectation(String name, boolean enabled) {
            this.name = name;
            this.enabled = enabled;
        }

        Expectation(String name) {
            this(name, true);
        }

        @Override
        protected ListenableFuture<Event> delegate() {
            return future;
        }

        public void enable(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void complete(Event event) {
            future.set(event);
        }

        public void fail(Exception e) {
            future.setException(e);
        }

        public boolean wait(long timeout, TimeUnit unit) {
            try {
                future.get(timeout, unit);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }

        public boolean onEvent(Event event) throws Exception {
            return true;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class ExpectationListener implements EventListener {
        private final Set<Expectation> expected = Sets.newCopyOnWriteArraySet();
        private final List<Event> unexpected = Lists.newCopyOnWriteArrayList();

        private volatile Exception failed;

        public Expectation expect(Expectation expectation) {
            if (failed == null) {
                expected.add(expectation);
            } else {
                expectation.fail(failed);
            }
            return expectation;
        }

        public Future<Event> expect(final String path, final int type) {
            return expect(new Expectation("path = " + path + ", type = " + type) {
                @Override
                public boolean onEvent(Event event) throws RepositoryException {
                    return type == event.getType() && equal(path, event.getPath());
                }
            });
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

        public List<Expectation> getMissing(int time, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException {
            List<Expectation> missing = Lists.newArrayList();
            try {
                Futures.allAsList(expected).get(time, timeUnit);
            }
            catch (TimeoutException e) {
                for (Expectation exp : expected) {
                    if (!exp.isDone()) {
                        missing.add(exp);
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
                    boolean found = false;
                    for (Expectation exp : expected) {
                        if (exp.isEnabled() && exp.onEvent(event)) {
                            found = true;
                            expected.remove(exp);
                            exp.complete(event);
                        }
                    }
                    if (!found) {
                        unexpected.add(event);
                    }

                }
            } catch (Exception e) {
                for (Expectation exp : expected) {
                    exp.fail(e);
                }
                failed = e;
            }
        }

        private static String key(String path, int type) {
            return path + ':' + type;
        }
    }

}
