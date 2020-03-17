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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeTraverser;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.fixture.DocumentMemoryFixture;
import org.apache.jackrabbit.oak.fixture.MemoryFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RecursiveDeleteTest {
    private final NodeStoreFixture fixture;
    private final NodeStore nodeStore;
    private String testNodePath =  "/content/testNode";
    private Random rnd = new Random();
    private int maxBucketSize = 100;
    private int maxDepth = 4;

    public RecursiveDeleteTest(NodeStoreFixture fixture) {
        this.nodeStore = fixture.createNodeStore();
        this.fixture = fixture;
    }

    @After
    public void tearDown(){
        fixture.dispose(nodeStore);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        result.add(new Object[]{new MemoryFixture()});
        result.add(new Object[]{new DocumentMemoryFixture()});
        return result;
    }

    @Test
    public void recursiveDelete() throws Exception{
        int actualCount = createSubtree(10000);
        assertEquals(actualCount, getSubtreeCount(getNode(nodeStore.getRoot(), testNodePath)));

        RecursiveDelete rd = new RecursiveDelete(nodeStore, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
        rd.setBatchSize(100);
        rd.run(testNodePath);

        assertEquals(actualCount, rd.getNumRemoved());
        assertFalse(getNode(nodeStore.getRoot(), testNodePath).exists());

        System.out.println(rd.getMergeCount());
        System.out.println(actualCount);
    }

    @Test
    public void multiplePaths() throws Exception{
        int count  = 121;
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("c");
        for (int i = 0; i < count; i++) {
            nb.child("a").child("c"+i);
            nb.child("b").child("c"+i);
        }
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        RecursiveDelete rd = new RecursiveDelete(nodeStore, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
        rd.setBatchSize(50);
        rd.run(asList("/a", "/b"));

        assertEquals(5, rd.getMergeCount());
        assertEquals(2 * count + 2, rd.getNumRemoved());
        assertFalse(getNode(nodeStore.getRoot(), "/a").exists());
        assertFalse(getNode(nodeStore.getRoot(), "/b").exists());
        assertTrue(getNode(nodeStore.getRoot(), "/c").exists());
    }

    private int createSubtree(int maxNodesCount) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder child = TestUtil.child(builder, testNodePath);
        AtomicInteger maxNodes = new AtomicInteger(maxNodesCount);
        int actualCount = createChildren(child, maxNodes, 0);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return actualCount + 1;
    }

    private int createChildren(NodeBuilder child, AtomicInteger maxNodes, int depth) {
        if (maxNodes.get() <= 0 || depth > maxDepth) {
            return 0;
        }

        int totalCount = 0;
        int childCount = rnd.nextInt(maxBucketSize);
        if (childCount == 0) {
            childCount = 1;
        }

        List<NodeBuilder> children = new ArrayList<>();
        for (int i = 0; i < childCount && maxNodes.get() > 0; i++){
            maxNodes.decrementAndGet();
            totalCount++;
            children.add(child.child("c"+i));

        }

        for (NodeBuilder c : children) {
            totalCount += createChildren(c, maxNodes, depth + 1);
        }

        return totalCount;
    }

    private int getSubtreeCount(NodeState state){
        TreeTraverser<NodeState> t = new TreeTraverser<NodeState>() {
            @Override
            public Iterable<NodeState> children(NodeState root) {
                return Iterables.transform(root.getChildNodeEntries(), ChildNodeEntry::getNodeState);
            }
        };
        return t.preOrderTraversal(state).size();
    }

}