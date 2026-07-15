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
package org.apache.jackrabbit.oak.plugins.migration;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeStateTestUtils {

    private NodeStateTestUtils() {
        // no instances
    }

    public static NodeStore createNodeStoreWithContent(String... paths) throws CommitFailedException, IOException {
        final NodeStore store = new MemoryNodeStore();
        final NodeBuilder builder = store.getRoot().builder();
        for (String path : paths) {
            create(builder, path);
        }
        commit(store, builder);
        return store;
    }

    public static void create(NodeBuilder rootBuilder, String path, PropertyState... properties) {
        final NodeBuilder builder = createOrGetBuilder(rootBuilder, path);
        for (PropertyState property : properties) {
            builder.setProperty(property);
        }
    }

    public static void commit(NodeStore store, NodeBuilder rootBuilder) throws CommitFailedException {
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    public static NodeState getNodeState(NodeState state, String path) {
        NodeState current = state;
        for (final String name : PathUtils.elements(path)) {
            current = current.getChildNode(name);
        }
        return current;
    }

    public static NodeBuilder createOrGetBuilder(NodeBuilder builder, String path) {
        NodeBuilder current = builder;
        for (final String name : PathUtils.elements(path)) {
            current = current.child(name);
        }
        return current;
    }

    public static void assertExists(NodeState state, String relPath) {
        assertTrue(relPath + " should exist", getNodeState(state, relPath).exists());
    }

    public static void assertMissing(NodeState state, String relPath) {
        assertFalse(relPath + " should not exist", getNodeState(state, relPath).exists());
    }

    public static ExpectedDifference expectDifference() {
        return new ExpectedDifference();
    }

    public static class ExpectedDifference {

        private ExpectedDifference() {
        }

        private final Map<String, Set<String>> expected = new HashMap<String, Set<String>>();

        public void verify(NodeState before, NodeState after) {
            final Map<String, Set<String>> actual = TestValidator.compare(before, after);
            for (String type : expected.keySet()) {
                if (!actual.containsKey(type)) {
                    actual.put(type, Collections.<String>emptySet());
                }
                assertEquals(type, expected.get(type), actual.get(type));
            }
        }

        public ExpectedDifference propertyAdded(String... paths) {
            return expect("propertyAdded", paths);
        }

        public ExpectedDifference propertyChanged(String... paths) {
            return expect("propertyChanged", paths);
        }

        public ExpectedDifference propertyDeleted(String... paths) {
            return expect("propertyDeleted", paths);
        }

        public ExpectedDifference childNodeAdded(String... paths) {
            return expect("childNodeAdded", paths);
        }

        public ExpectedDifference childNodeChanged(String... paths) {
            return expect("childNodeChanged", paths);
        }

        public ExpectedDifference childNodeDeleted(String... paths) {
            return expect("childNodeDeleted", paths);
        }

        public ExpectedDifference strict() {
            return this.propertyAdded()
                    .propertyChanged()
                    .propertyDeleted()
                    .childNodeAdded()
                    .childNodeChanged()
                    .childNodeDeleted();
        }

        private ExpectedDifference expect(String type, String... paths) {
            if (!expected.containsKey(type)) {
                expected.put(type, new TreeSet<String>());
            }
            Collections.addAll(expected.get(type), paths);
            return this;
        }
    }

    private static class TestValidator extends DefaultValidator {

        final Map<String, Set<String>> actual = new HashMap<String, Set<String>>();

        String path = "/";

        public static Map<String, Set<String>> compare(NodeState before, NodeState after) {
            final TestValidator validator = new TestValidator();
            EditorDiff.process(validator, before, after);
            return validator.actual;
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            path = PathUtils.getParentPath(path);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            record("propertyAdded", PathUtils.concat(path, after.getName()));
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            record("propertyChanged", PathUtils.concat(path, after.getName()));
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            record("propertyDeleted", PathUtils.concat(path, before.getName()));
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            path = PathUtils.concat(path, name);
            record("childNodeAdded", path);
            return this;
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after)
                throws CommitFailedException {
            // make sure not to record false positives (inefficient for large trees)
            if (!before.equals(after)) {
                path = PathUtils.concat(path, name);
                record("childNodeChanged", path);
                return this;
            }
            return null;
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            path = PathUtils.concat(path, name);
            record("childNodeDeleted", path);
            return this;
        }

        private void record(String type, String path) {
            if (!actual.containsKey(type)) {
                actual.put(type, new TreeSet<String>());
            }
            actual.get(type).add(path);
        }
    }
}
