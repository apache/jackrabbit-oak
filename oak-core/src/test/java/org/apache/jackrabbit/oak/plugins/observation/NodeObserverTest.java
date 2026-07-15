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
package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class NodeObserverTest {
    private final NodeState before;

    {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("m").setChildNode("n").setProperty("p", 1);
        builder.getChildNode("m").getChildNode("n").setProperty("extra", 42);
        builder.getChildNode("m").getChildNode("n").setChildNode("o").setProperty("q", 2);
        builder.setChildNode("a").setChildNode("b").setProperty("p", 1);
        before = builder.getNodeState();
    }

    private TestNodeObserver nodeObserver;

    @Before
    public void setup() {
        nodeObserver = new TestNodeObserver("/m/n", "extra");
        nodeObserver.contentChanged(before, CommitInfo.EMPTY);
    }

    @Test
    public void addNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").setChildNode("new").setProperty("p", "3");
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertEquals(Map.of("/m/n/new", Set.of("p")), nodeObserver.added);
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
        assertTrue(nodeObserver.properties.isEmpty());
    }

    @Test
    public void deleteNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").getChildNode("o").remove();
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertEquals(Map.of("/m/n/o", Set.of("q")), nodeObserver.deleted);
        assertTrue(nodeObserver.changed.isEmpty());
        assertTrue(nodeObserver.properties.isEmpty());
    }

    @Test
    public void changeNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").setProperty("p", 42);
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertEquals(Map.of("/m/n", Set.of("p")), nodeObserver.changed);
        assertEquals(Map.of("/m/n", Map.of("extra", "42")), nodeObserver.properties);
    }

    @Test
    public void ignoreAdd() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").setChildNode("new").setProperty("p", "3");
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
        assertTrue(nodeObserver.properties.isEmpty());
    }

    @Test
    public void ignoreDelete() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").remove();
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
        assertTrue(nodeObserver.properties.isEmpty());
    }

    @Test
    public void ignoreChange() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").setProperty("p", 42);
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
        assertTrue(nodeObserver.properties.isEmpty());
    }

    //------------------------------------------------------------< TestNodeObserver >---

    private static class TestNodeObserver extends NodeObserver {
        private final Map<String, Set<String>> added = new HashMap<>();
        private final Map<String, Set<String>> deleted = new HashMap<>();
        private final Map<String, Set<String>> changed = new HashMap<>();
        private final Map<String, Map<String, String>> properties = new HashMap<>();

        protected TestNodeObserver(String path, String... propertyNames) {
            super(path, propertyNames);
        }

        @Override
        protected void added(
                @NotNull String path,
                @NotNull Set<String> added,
                @NotNull Set<String> deleted,
                @NotNull Set<String> changed,
                @NotNull Map<String, String> properties,
                @NotNull CommitInfo commitInfo) {
            this.added.put(path, new HashSet<>(added));
            if (!properties.isEmpty()) {
                this.properties.put(path, new HashMap<>(properties));
            }
        }

        @Override
        protected void deleted(
                @NotNull String path,
                @NotNull Set<String> added,
                @NotNull Set<String> deleted,
                @NotNull Set<String> changed,
                @NotNull Map<String, String> properties,
                @NotNull CommitInfo commitInfo) {
            this.deleted.put(path, new HashSet< >(deleted));
            if (!properties.isEmpty()) {
                this.properties.put(path, new HashMap<>(properties));
            }
        }

        @Override
        protected void changed(
                @NotNull String path,
                @NotNull Set<String> added,
                @NotNull Set<String> deleted,
                @NotNull Set<String> changed,
                @NotNull Map<String, String> properties,
                @NotNull CommitInfo commitInfo) {
            this.changed.put(path, new HashSet<>(changed));
            if (!properties.isEmpty()) {
                this.properties.put(path, new HashMap<>(properties));
            }
        }

    }
}
