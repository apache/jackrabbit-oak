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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrBaseTest;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.index.SolrCommitHook}
 */
public class SolrCommitHookTest extends SolrBaseTest {

    private KernelNodeStore store;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MicroKernel microKernel = new MicroKernelImpl();
        store = new KernelNodeStore(microKernel);
    }

    @Test
    public void testNothingHappened() throws Exception {
        SolrCommitHook solrCommitHook = new SolrCommitHook(server);
        NodeState before = store.getRoot().builder().getNodeState();
        NodeState after = store.getRoot().builder().getNodeState();
        NodeState changedState = solrCommitHook.processCommit(before, after);
        assertEquals(after, changedState);
    }

    @Test
    public void testAddNode() throws Exception {
        SolrCommitHook solrCommitHook = new SolrCommitHook(server);
        NodeState before = store.getRoot().builder().getNodeState();
        NodeState after = store.getRoot().builder().child("somechild").getNodeState();
        NodeState changedState = solrCommitHook.processCommit(before, after);
        assertEquals(after, changedState);
    }

    @Test
    public void testRemoveNode() throws Exception {
        SolrCommitHook solrCommitHook = new SolrCommitHook(server);
        NodeState before = store.getRoot().builder().child("somechild").getNodeState();
        NodeState after = before.builder().removeChildNode("somechild").getNodeState();
        NodeState changedState = solrCommitHook.processCommit(before, after);
        assertEquals(after, changedState);
    }

    @Test
    public void testPropertyAdded() throws Exception {
        SolrCommitHook solrCommitHook = new SolrCommitHook(server);
        NodeState before = store.getRoot().builder().getNodeState();
        NodeState after = before.builder().setProperty("p", "v").getNodeState();
        NodeState changedState = solrCommitHook.processCommit(before, after);
        assertEquals(after, changedState);
    }

    @Test
    public void testPropertyRemoved() throws Exception {
        SolrCommitHook solrCommitHook = new SolrCommitHook(server);
        NodeState before = store.getRoot().builder().setProperty("p", "v").getNodeState();
        NodeState after = before.builder().removeProperty("p").getNodeState();
        NodeState changedState = solrCommitHook.processCommit(before, after);
        assertEquals(after, changedState);
    }

}
