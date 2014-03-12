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

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

public class NodeObserverTest {
    private final NodeState before;

    {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("m").setChildNode("n").setProperty("p", 1);
        builder.getChildNode("m").getChildNode("n").setChildNode("o").setProperty("q", 2);
        builder.setChildNode("a").setChildNode("b").setProperty("p", 1);
        before = builder.getNodeState();
    }

    private TestNodeObserver nodeObserver;

    @Before
    public void setup() {
        nodeObserver = new TestNodeObserver("/m/n");
        nodeObserver.contentChanged(before, CommitInfo.EMPTY);
    }

    @Test
    public void addNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").setChildNode("new").setProperty("p", "3");
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertEquals(ImmutableMap.of("/m/n/new", ImmutableSet.of("p")), nodeObserver.added);
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
    }

    @Test
    public void deleteNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").getChildNode("o").remove();
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertEquals(ImmutableMap.of("/m/n/o", ImmutableSet.of("q")), nodeObserver.deleted);
        assertTrue(nodeObserver.changed.isEmpty());
    }

    @Test
    public void changeNode() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("m").getChildNode("n").setProperty("p", 42);
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertEquals(ImmutableMap.of("/m/n", ImmutableSet.of("p")), nodeObserver.changed);
    }

    @Test
    public void ignoreAdd() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").setChildNode("new").setProperty("p", "3");
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
    }

    @Test
    public void ignoreDelete() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").remove();
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
    }

    @Test
    public void ignoreChange() {
        NodeBuilder builder = before.builder();
        builder.getChildNode("a").getChildNode("b").setProperty("p", 42);
        nodeObserver.contentChanged(builder.getNodeState(), CommitInfo.EMPTY);

        assertTrue(nodeObserver.added.isEmpty());
        assertTrue(nodeObserver.deleted.isEmpty());
        assertTrue(nodeObserver.changed.isEmpty());
    }

    //------------------------------------------------------------< TestNodeObserver >---

    private static class TestNodeObserver extends NodeObserver {
        private final Map<String, Set<String>> added = Maps.newHashMap();
        private final Map<String, Set<String>> deleted = Maps.newHashMap();
        private final Map<String, Set<String>> changed = Maps.newHashMap();

        protected TestNodeObserver(String path) {
            super(path);
        }

        @Override
        protected void added(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull CommitInfo commitInfo) {
            update(this.added, path, added);
        }

        @Override
        protected void deleted(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull CommitInfo commitInfo) {
            update(this.deleted, path, deleted);
        }

        @Override
        protected void changed(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull CommitInfo commitInfo) {
            update(this.changed, path, changed);
        }

        private static void update(Map<String, Set<String>> map, String key, Set<String> value) {
            Set<String> current = map.get(key);
            if (current == null) {
                current = Sets.newHashSet();
            }
            current.addAll(value);
            map.put(key, current);
        }
    }
}
