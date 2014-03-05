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
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Map;
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
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.Selectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObservationTest extends AbstractRepositoryTest {
    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    private static final String TEST_NODE = "test_node";
    private static final String REFERENCEABLE_NODE = "\"referenceable\"";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final String TEST_TYPE = "mix:test";
    public static final int TIME_OUT = 4;

    private Session observingSession;
    private ObservationManager observationManager;
    private String test_uuid;

    public ObservationTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();

        NodeTypeManager ntMgr = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName(TEST_TYPE);
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node n = session.getRootNode().addNode(TEST_NODE);
        n.addMixin(TEST_TYPE);
        Node refNode = n.addNode(REFERENCEABLE_NODE);
        refNode.addMixin(JcrConstants.MIX_REFERENCEABLE);
        test_uuid = refNode.getProperty(JcrConstants.JCR_UUID).getString();

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

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
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

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
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

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectAdd(n.addNode("n2"));
            listener.expectRemove(n.getNode("n1")).remove();
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void typeFilter() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null,
                new String[]{TEST_TYPE}, false);

        try {
            Node n = getNode(TEST_PATH);
            Property p = n.setProperty("p", "v");
            listener.expectAdd(p);
            Node n1 = n.addNode("n1");
            listener.expect(n1.getPath(), NODE_ADDED);
            n1.addNode("n2");
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectChange(p).setValue("v2");
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());

            listener.expectRemove(p).remove();
            getAdminSession().save();

            missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void uuidFilter() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true,
                new String[]{test_uuid}, null, false);

        try {
            Node nonRefNode = getNode(TEST_PATH);
            Node refNode = nonRefNode.getNode(REFERENCEABLE_NODE);

            nonRefNode.addNode("n");
            listener.expect(refNode.addNode("r").getPath(), NODE_ADDED);
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    @Test
    public void identifier() throws RepositoryException, InterruptedException, ExecutionException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_ADDED, TEST_PATH, true, null, null, false);
        try {
            Node n = getNode(TEST_PATH);
            listener.expect(new Expectation("Has correct id") {
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return (TEST_PATH + "/newNode").equals(event.getIdentifier());
                }
            });

            n.addNode("newNode");
            getAdminSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
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

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
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

        for (boolean deep : new boolean[]{false, true}) {
            Node root = getNode("/");
            if (root.hasNode("events")) {
                root.getNode("events").remove();
                root.getSession().save();
            }

            ExpectationListener listener = new ExpectationListener();
            observationManager.addEventListener(listener, NODE_ADDED, path, deep, null, null, false);
            try {
                root.addNode("events").addNode("only").addNode("here").addNode("below").addNode("this");
                listener.expect("/events/only/here/below", NODE_ADDED);
                if (deep) {
                    listener.expect("/events/only/here/below/this", NODE_ADDED);
                }
                root.getSession().save();

                List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
                assertTrue("Missing events: " + missing, missing.isEmpty());
                List<Event> unexpected = listener.getUnexpected();
                assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
            }
            finally {
                observationManager.removeEventListener(listener);
            }
        }
    }

    @Test
    public void pathFilterWithTrailingSlash() throws Exception {
        final String path = "/events/only/here";
        ExpectationListener listener = new ExpectationListener();
        listener.expect(new Expectation(path){
            @Override
            public boolean onEvent(Event event) throws Exception {
                return PathUtils.isAncestor(path, event.getPath());
            }
        });

        observationManager.addEventListener(listener, NODE_ADDED, path + '/', true, null, null, false);
        try {
            Node root = getNode("/");
            root.addNode("events").addNode("only").addNode("here").addNode("at");
            root.getSession().save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
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
        assertNotNull(hasEvents.get(TIME_OUT, TimeUnit.SECONDS));

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

    @Test
    public void testMove() throws RepositoryException, ExecutionException, InterruptedException {
        Node testNode = getNode(TEST_PATH);
        Session session = testNode.getSession();
        Node nodeA = testNode.addNode("a");
        Node nodeAA = nodeA.addNode("aa");
        Node nodeT = testNode.addNode("t");
        Node nodeS = testNode.addNode("s");
        session.save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_MOVED, "/", true, null, null, false);

        String src1 = nodeA.getPath();
        String dst1 = nodeT.getPath() + "/b";
        session.move(src1, dst1);
        listener.expectMove(src1, dst1);

        String src2 = nodeT.getPath() + "/b/aa";
        String dst2 = nodeS.getPath() + "/bb";
        session.move(src2, dst2);
        listener.expectMove(src1 + "/aa", dst2);

        session.save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void testReorder() throws RepositoryException, InterruptedException, ExecutionException {
        Node testNode = getNode(TEST_PATH);
        Node nodeA = testNode.addNode("a", "nt:unstructured");
        Node nodeB = testNode.addNode("b", "nt:unstructured");
        testNode.getSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, NODE_MOVED, "/", true, null, null, false);
        listener.expect(new Expectation("orderBefore") {
            @Override
            public boolean onEvent(Event event) throws Exception {
                if (event.getType() != NODE_MOVED || event.getInfo() == null) {
                    return false;
                }

                Map<?, ?> info = event.getInfo();
                if (PathUtils.concat(TEST_PATH, "a").equals(event.getPath())) {
                    return "a".equals(info.get("srcChildRelPath")) &&
                            "b".equals(info.get("destChildRelPath"));
                } else if (PathUtils.concat(TEST_PATH, "b").equals(event.getPath())) {
                    return "b".equals(info.get("srcChildRelPath")) &&
                            "a".equals(info.get("destChildRelPath"));
                } else {
                    return false;
                }
            }
        });

        testNode.orderBefore(nodeA.getName(), null);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void addSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        Node n = getNode(TEST_PATH);
        Node a = listener.expectAdd(n.addNode("a"));
        Node b = listener.expectAdd(a.addNode("b"));
        listener.expectAdd(b.addNode("c"));
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void removeSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        n.addNode("a").addNode("b").addNode("c");
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        listener.expectRemove(n.getNode("a")).remove();
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void moveSubtree() throws RepositoryException, ExecutionException, InterruptedException {
        Node n = getNode(TEST_PATH);
        n.addNode("a").addNode("b").addNode("c");
        getAdminSession().save();

        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);

        getAdminSession().move(TEST_PATH + "/a", TEST_PATH + "/t");
        listener.expect(TEST_PATH + "/t", NODE_MOVED);
        listener.expect(TEST_PATH + "/a", NODE_REMOVED);
        listener.expect(TEST_PATH + "/a/jcr:primaryType", PROPERTY_REMOVED);
        listener.expect(TEST_PATH + "/t", NODE_ADDED);
        getAdminSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterDisjunctPaths()
            throws ExecutionException, InterruptedException, RepositoryException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();
        builder.condition(builder.any(
                builder.path(TEST_PATH + "/a/b"),
                builder.path(TEST_PATH + "/x/y")));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node b = testNode.addNode("a").addNode("b");
        b.addNode("c");
        Node y = testNode.addNode("x").addNode("y");
        y.addNode("z");

        listener.expect(b.getPath(), NODE_ADDED);
        listener.expect(y.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterPropertyOfParent()
            throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Events for all items whose parent has a property named "foo" with value "bar"
        builder.condition(builder.property(Selectors.PARENT, "foo",
                new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState property) {
                        return "bar".equals(property.getValue(STRING));
                    }
                }));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = testNode.addNode("a");
        Node x = testNode.addNode("x");
        a.setProperty("foo", "bar");
        x.setProperty("foo", "baz");
        a.addNode("b");
        x.addNode("y");

        listener.expect(a.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
        listener.expect(a.getPath() + "/foo", PROPERTY_ADDED);
        listener.expect(a.getPath() + "/b", NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void filterPropertyOfChild()
            throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Events for all items that have a property "b/c/foo" with value "bar"
        builder.condition(builder.property(Selectors.fromThis("b/c"), "foo",
                new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState property) {
                return "bar".equals(property.getValue(STRING));
            }
        }));
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = testNode.addNode("a");
        a.addNode("b").addNode("c").setProperty("foo", "bar");
        a.addNode("d");
        Node x = testNode.addNode("x");
        x.addNode("b").addNode("c").setProperty("foo", "baz");
        x.addNode("d");

        listener.expect(a.getPath(), NODE_ADDED);
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void addSubtreeFilter() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);
        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Only generate events for the root of added sub trees
        builder.condition(builder.addSubtree());
        oManager.addEventListener(listener, builder.build());

        Node testNode = getNode(TEST_PATH);
        Node a = listener.expectAdd(testNode.addNode("a"));
        a.addNode("c");
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
    }

    @Test
    public void removeSubtreeFilter() throws RepositoryException, ExecutionException, InterruptedException {
        assumeTrue(observationManager instanceof ObservationManagerImpl);

        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a").addNode("c");
        testNode.getSession().save();

        ObservationManagerImpl oManager = (ObservationManagerImpl) observationManager;
        ExpectationListener listener = new ExpectationListener();
        FilterBuilder builder = new FilterBuilder();

        // Only generate events for the root of deleted sub trees
        builder.condition(builder.deleteSubtree());
        oManager.addEventListener(listener, builder.build());

        listener.expectRemove(testNode.getNode("a")).remove();
        testNode.getSession().save();

        List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
        assertTrue("Missing events: " + missing, missing.isEmpty());
        List<Event> unexpected = listener.getUnexpected();
        assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
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
        private final Set<Expectation> expected = synchronizedSet(
                Sets.<Expectation>newCopyOnWriteArraySet());
        private final List<Event> unexpected = synchronizedList(
                Lists.<Event>newCopyOnWriteArrayList());

        private volatile Exception failed;

        public Expectation expect(Expectation expectation) {
            if (failed != null) {
                expectation.fail(failed);
            }
            expected.add(expectation);
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

        public void expectMove(final String src, final String dst) {
            expect(new Expectation(">" + src + ':' + dst){
                @Override
                public boolean onEvent(Event event) throws Exception {
                    return event.getType() == NODE_MOVED &&
                            equal(dst, event.getPath()) &&
                            equal(src, event.getInfo().get("srcAbsPath")) &&
                            equal(dst, event.getInfo().get("destAbsPath"));
                }
            });
        }

        public List<Expectation> getMissing(int time, TimeUnit timeUnit)
                throws ExecutionException, InterruptedException {
            List<Expectation> missing = Lists.newArrayList();
            long t0 = System.nanoTime();
            try {
                Futures.allAsList(expected).get(time, timeUnit);
            }
            catch (TimeoutException e) {
                long dt = System.nanoTime() - t0;
                // TODO remove again once OAK-1491 is fixed
                assertTrue("Spurious wak-up after " + 0,
                        dt < 0.8*TimeUnit.NANOSECONDS.convert(time, timeUnit));
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
