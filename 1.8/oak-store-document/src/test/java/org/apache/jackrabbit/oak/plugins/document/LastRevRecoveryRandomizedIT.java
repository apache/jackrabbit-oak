/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.comparePropertiesAgainstBaseState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A randomized test for _lastRev recovery.
 */
public class LastRevRecoveryRandomizedIT {

    private static final Logger LOG = LoggerFactory.getLogger(LastRevRecoveryRandomizedIT.class);
    private static final int SEED = Integer.getInteger(
            LastRevRecoveryRandomizedIT.class.getSimpleName() + "-seed",
            new Random().nextInt());

    private Random random = new Random();

    private MemoryDocumentStore store;

    private DocumentNodeStore ns;

    private Map<String, NodeState> currentState = Maps.newHashMap();

    private DocumentRootBuilder builder;

    private Map<String, NodeState> pending = Maps.newHashMap();

    private int counter = 0;

    private List<String> ops = Lists.newArrayList();

    private Clock clock;

    @Before
    public void setUp() throws Exception {
        LOG.info("Running " + getClass().getSimpleName() + " with seed " + SEED);
        clock = new Clock.Virtual();
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        random.setSeed(SEED);
        store = new MemoryDocumentStore();
        ns = new DocumentMK.Builder().setDocumentStore(store)
                .setLeaseCheck(false).clock(clock)
                .setAsyncDelay(0).getNodeStore();
        builder = newBuilder(ns);
        builder.child("root");
        merge(ns, builder);
        currentState.put("/root", ns.getRoot().getChildNode("root"));
        builder = newBuilder(ns);
    }

    @After
    public void tearDown() throws Exception {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
        ns.dispose();
    }

    @Test
    public void randomized() throws Exception {
        for (int i = 0; i < 1000; i++) {
            boolean success = false;
            try {
                switch (random.nextInt(10)) {
                    case 0:
                    case 1:
                    case 2:
                        addNode();
                        break;
                    case 3:
                        addLeafNode();
                        break;
                    case 4:
                        removeNode();
                        break;
                    case 5:
                    case 6:
                        setProperty();
                        break;
                    case 7:
                        merge();
                        break;
                    case 8:
                        purge();
                        break;
                    case 9:
                        bgOp();
                        break;
                }
                checkStore();
                success = true;
            } finally {
                if (!success) {
                    int num = 0;
                    for (String line : ops) {
                        System.out.println(num++ + ": " + line);
                    }
                }
            }
        }
    }

    private void bgOp() {
        ops.add("runBackgroundOperations()");
        ns.runBackgroundOperations();
    }

    private void purge() {
        ops.add("purge()");
        builder.purge();
    }

    private void merge() throws CommitFailedException {
        ops.add("merge()");
        merge(ns, builder);
        for (Map.Entry<String, NodeState> entry : pending.entrySet()) {
            if (entry.getValue() == null) {
                currentState.remove(entry.getKey());
            } else {
                currentState.put(entry.getKey(), entry.getValue());
            }
        }
        pending.clear();
        builder = newBuilder(ns);
    }

    private void setProperty() {
        String p = choosePath();
        String name = "p-" + counter++;
        ops.add("setProperty() " + PathUtils.concat(p, name));
        NodeBuilder ns = getNode(p);
        ns.setProperty(name, "v");
        pending.put(p, ns.getNodeState());
    }

    private void removeNode() {
        String p = choosePath();
        if (p.equals("/root")) {
            return;
        }
        ops.add("removeNode() " + p);
        getNode(p).remove();
        pending.put(p, null);
    }

    private void addNode() {
        String p = choosePath();
        List<String> elements = Lists.newArrayList(PathUtils.elements(p));
        if (elements.size() > 2) {
            elements = elements.subList(1, elements.size() - 1);
            elements = elements.subList(0, random.nextInt(elements.size() + 1));
            p = PathUtils.concat("/root", elements.toArray(new String[elements.size()]));
        }
        String name = "n-" + counter++;
        ops.add("addNode() " + PathUtils.concat(p, name));
        NodeBuilder ns = getNode(p);
        pending.put(PathUtils.concat(p, name), ns.child(name).getNodeState());
    }

    private void addLeafNode() {
        String p = choosePath();
        String name = "n-" + counter++;
        ops.add("addLeafNode() " + PathUtils.concat(p, name));
        NodeBuilder ns = getNode(p);
        pending.put(PathUtils.concat(p, name), ns.child(name).getNodeState());
    }

    private NodeBuilder getNode(String path) {
        NodeBuilder node = builder;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }
        if (!node.exists()) {
            throw new IllegalStateException("node does not exist: " + path);
        }
        return node;
    }

    private String choosePath() {
        String path = "/root";
        String next;
        while ((next = chooseNode(path)) != null) {
            path = next;
        }
        return path;
    }

    private String chooseNode(String parentPath) {
        NodeBuilder node = getNode(parentPath);

        int numChildren = (int) node.getChildNodeCount(Long.MAX_VALUE);
        if (numChildren == 0) {
            return null;
        }
        int k = random.nextInt(numChildren);
        int c = 0;
        for (String name : node.getChildNodeNames()) {
            if (c++ == k) {
                return PathUtils.concat(parentPath, name);
            }
        }

        return null;
    }

    private void checkStore() {
        MemoryDocumentStore s = store.copy();
        // force lease expire
        UpdateOp op = new UpdateOp(String.valueOf(ns.getClusterId()), false);
        op.set(ClusterNodeInfo.LEASE_END_KEY, clock.getTime() - 1000);
        if (s.findAndUpdate(Collection.CLUSTER_NODES, op) == null) {
            fail("failed to set lease end");
        }
        // will trigger recovery on startup
        DocumentNodeStore dns = new DocumentMK.Builder()
                .setClusterId(ns.getClusterId())
                .clock(clock).setLeaseCheck(false)
                .setDocumentStore(s).setAsyncDelay(0).getNodeStore();
        Map<String, NodeState> states = Maps.newHashMap(currentState);
        NodeState root = dns.getRoot().getChildNode("root");
        compareAndTraverse(root, "/root", states);
        assertTrue("missing nodes: " + states.keySet() + " (seed=" + SEED + ")",
                states.isEmpty());
        dns.dispose();
    }

    private void compareAndTraverse(NodeState state,
                                    final String path,
                                    Map<String, NodeState> states) {
        NodeState expected = states.remove(path);
        if (expected == null) {
            fail("unexpected node at " + path + " (seed=" + SEED + ")");
            return;
        }
        comparePropertiesAgainstBaseState(state, expected, new DefaultNodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                fail("unexpected property: " + path + "/" + after + " (seed=" + SEED + ")");
                return super.propertyAdded(after);
            }

            @Override
            public boolean propertyChanged(PropertyState before,
                                           PropertyState after) {
                assertEquals("property mismatch on node " + path + " (seed=" + SEED + ")",
                        before, after);
                return super.propertyChanged(before, after);
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                fail("missing property: " + path + "/" + before + " (seed=" + SEED + ")");
                return super.propertyDeleted(before);
            }
        });
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            String p = PathUtils.concat(path, entry.getName());
            compareAndTraverse(entry.getNodeState(), p, states);
        }
    }

    private static DocumentRootBuilder newBuilder(DocumentNodeStore store) {
        return (DocumentRootBuilder) store.getRoot().builder();
    }

    private static void merge(NodeStore ns, NodeBuilder builder)
            throws CommitFailedException {
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
