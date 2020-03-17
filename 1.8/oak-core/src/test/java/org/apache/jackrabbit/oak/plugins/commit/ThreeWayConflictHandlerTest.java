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
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Test;

public class ThreeWayConflictHandlerTest {

    @Test
    public void addExistingProperty() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
                called.set(true);
                assertEquals("ours", ours.getValue(STRING));
                assertEquals("theirs", theirs.getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").setProperty("p0", "theirs");
        ourRoot.getTree("/c").setProperty("p0", "ours");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void changeDeletedProperty() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours, PropertyState base) {
                called.set(true);
                assertEquals("ours", ours.getValue(STRING));
                assertEquals("base", base.getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").removeProperty("p");
        ourRoot.getTree("/c").setProperty("p", "ours");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void changeChangedProperty() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
                    PropertyState base) {
                called.set(true);
                assertEquals("ours", ours.getValue(STRING));
                assertEquals("theirs", theirs.getValue(STRING));
                assertEquals("base", base.getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").setProperty("p", "theirs");
        ourRoot.getTree("/c").setProperty("p", "ours");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void deleteDeletedProperty() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState base) {
                called.set(true);
                assertEquals("base", base.getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").removeProperty("p");
        ourRoot.getTree("/c").removeProperty("p");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void deleteChangedProperty() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs, PropertyState base) {
                called.set(true);
                assertEquals("theirs", theirs.getValue(STRING));
                assertEquals("base", base.getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").setProperty("p", "theirs");
        ourRoot.getTree("/c").removeProperty("p");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void changeDeletedNode() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours, NodeState base) {
                called.set(true);
                assertTrue(ours.hasProperty("p"));
                assertTrue(base.hasProperty("p"));
                assertEquals("ours", ours.getProperty("p").getValue(STRING));
                assertEquals("base", base.getProperty("p").getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").remove();
        ourRoot.getTree("/c").setProperty("p", "ours");

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void deleteChangedNode() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs, NodeState base) {
                called.set(true);
                assertTrue(theirs.hasProperty("p"));
                assertTrue(base.hasProperty("p"));
                assertEquals("theirs", theirs.getProperty("p").getValue(STRING));
                assertEquals("base", base.getProperty("p").getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").setProperty("p", "theirs");
        ourRoot.getTree("/c").remove();

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    @Test
    public void deleteDeletedNode() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreeWayConflictHandler handler = new ErrorThreeWayConflictHandler() {

            @Override
            public Resolution deleteDeletedNode(NodeBuilder parent, String name, NodeState base) {
                called.set(true);
                assertTrue(base.hasProperty("p"));
                assertEquals("base", base.getProperty("p").getValue(STRING));
                return Resolution.IGNORED;
            }
        };

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        theirRoot.getTree("/c").remove();
        ourRoot.getTree("/c").remove();

        theirRoot.commit();
        ourRoot.commit();

        assertTrue(called.get());
    }

    private static ContentRepository newRepo(ThreeWayConflictHandler handler) {
        return new Oak().with(new OpenSecurityProvider()).with(handler).createContentRepository();
    }

    private static Root login(ContentRepository repo) throws Exception {
        return repo.login(null, null).getLatestRoot();
    }

    private static void setup(Root root) throws Exception {
        Tree tree = root.getTree("/");
        tree.addChild("c").setProperty("p", "base");
        root.commit();
    }

    private static class ErrorThreeWayConflictHandler implements ThreeWayConflictHandler {

        @Override
        public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours, PropertyState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
                PropertyState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs, PropertyState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours, NodeState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs, NodeState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }

        @Override
        public Resolution deleteDeletedNode(NodeBuilder parent, String name, NodeState base) {
            Assert.fail("method should not be called");
            return Resolution.IGNORED;
        }
    }
}
