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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreIterator.BUFFER_MEM_LIMIT_CONFIG_NAME;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.TestUtils.createList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class FlatFileStoreIteratorTest {

    @Test
    public void simpleTraversal() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred);
        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState ns1 = a.getNodeState().getChildNode("jcr:content");
        assertEquals("/a/jcr:content", ns1.getString("path"));
        assertEquals(1, fitr.getBufferSize());

        NodeState ns2 = ns1.getChildNode("metadata");
        assertEquals("/a/jcr:content/metadata", ns2.getString("path"));
        assertEquals(2, fitr.getBufferSize());

        NodeStateEntry nse1 = fitr.next();
        assertEquals("/a/jcr:content", nse1.getPath());

        NodeStateEntry nse2 = fitr.next();
        assertEquals("/a/jcr:content/metadata", nse2.getPath());

        NodeStateEntry nse3 = fitr.next();
        assertEquals("/a/d", nse3.getPath());
        assertEquals(0, nse3.getNodeState().getChildNodeCount(100));

        NodeStateEntry nse4 = fitr.next();
        assertEquals("/e", nse4.getPath());
        assertEquals(0, nse4.getNodeState().getChildNodeCount(100));

        assertFalse(fitr.hasNext());
    }

    @Test
    public void invalidOrderAccess() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred);
        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState ns1 = a.getNodeState().getChildNode("jcr:content");

        NodeStateEntry nse1 = fitr.next();
        assertEquals("/a/jcr:content", nse1.getPath());
        assertEquals(1, nse1.getNodeState().getChildNodeCount(100));

        //Now move past /a/jcr:content
        NodeStateEntry nse2 = fitr.next();
        assertEquals("/a/jcr:content/metadata", nse2.getPath());

        try {
            //Now access from /a/jcr:content node should fail
            ns1.getChildNodeCount(100);
            fail("Access should have failed");
        } catch (IllegalStateException ignore) {

        }
    }

    // OAK-7284
    @Test
    public void comodificationException() {
        Set<String> preferred = ImmutableSet.of("j:c");

        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/j:c", "/a/j:c/j:c", "/a/b"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred);

        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState aNS = a.getNodeState();

        // fake aggregate rule like "j:c/*"
        for (ChildNodeEntry cne : aNS.getChildNodeEntries()) {
            NodeState childNS = cne.getNodeState();
            // read preferred names for aggregation sub-tree nodes
            for (String prefName : preferred) {
                childNS.getChildNode(prefName);
            }
        }
    }

    // OAK-7285
    @Test
    public void getChildNodeLimitedByNonPreferred() {
        // have more than 1 preferred names
        Set<String> preferred = ImmutableSet.of("j:c", "md");

        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/b", "/a/c"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred);

        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState aNS = a.getNodeState();
        aNS.getChildNode("j:c");

        // Don't read whole tree to conclude that "j:c" doesn't exist (reading /a/b should imply that it doesn't exist)
        assertEquals(1, fitr.getBufferSize());
    }

    @Test
    public void bufferEstimatesMemory() {
        List<NodeStateEntry> nseList = Lists.newArrayList(
                new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/a", 20),
                new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/a/b", 30)
        );
        FlatFileStoreIterator fitr = new FlatFileStoreIterator(nseList.iterator(), ImmutableSet.of());

        NodeStateEntry entry = fitr.next();
        NodeState entryNS = entry.getNodeState();
        assertEquals("/a", entry.getPath());
        assertEquals("Fetching from iterator doesn't use buffer", 0, fitr.getBufferMemoryUsage());

        entryNS.getChildNode("b");
        assertEquals(1, fitr.getBufferSize());
        assertEquals("Reaching child from node state should estimate 30 for /a/b",
                30, fitr.getBufferMemoryUsage());
    }

    @Test
    public void memUsageConfig() {
        String configuredValue = System.clearProperty(BUFFER_MEM_LIMIT_CONFIG_NAME);
        try {
            NodeStateEntry root = new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/");
            NodeStateEntry e1Byte = new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/a/b", 1);
            NodeStateEntry e1MB = new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/a", 1 * 1024 * 1024);
            NodeStateEntry e100MB = new NodeStateEntry(EmptyNodeState.EMPTY_NODE, "/a", 100 * 1024 * 1024);

            {
                //default configured limit
                List<NodeStateEntry> list = Lists.newArrayList(root, e100MB, e1Byte);
                FlatFileStoreIterator fitr = new FlatFileStoreIterator(list.iterator(), ImmutableSet.of());
                NodeState rootNS = fitr.next().getNodeState();
                NodeState aNS = rootNS.getChildNode("a");//default is 100MB, this should work
                try {
                    aNS.getChildNode("b");
                    fail("Reading beyond default 100MB must fail");
                } catch (IllegalStateException ise) {
                    //ignore
                }
            }

            {
                System.setProperty(BUFFER_MEM_LIMIT_CONFIG_NAME, "1");

                List<NodeStateEntry> list = Lists.newArrayList(root, e1MB, e1Byte);
                FlatFileStoreIterator fitr = new FlatFileStoreIterator(list.iterator(), ImmutableSet.of());
                NodeState rootNS = fitr.next().getNodeState();
                NodeState aNS = rootNS.getChildNode("a");//configured limit is 10 bytes, this should work
                try {
                    aNS.getChildNode("b");
                    fail("Reading beyond configured 1MB must fail");
                } catch (IllegalStateException ise) {
                    //ignore
                }
            }

            {
                // illegal config behaves as default
                System.setProperty(BUFFER_MEM_LIMIT_CONFIG_NAME, "1A");

                List<NodeStateEntry> list = Lists.newArrayList(root, e100MB, e1Byte);
                FlatFileStoreIterator fitr = new FlatFileStoreIterator(list.iterator(), ImmutableSet.of());
                NodeState rootNS = fitr.next().getNodeState();
                NodeState aNS = rootNS.getChildNode("a");//default is 100MB, this should work
                try {
                    aNS.getChildNode("b");
                    fail("Reading beyond default 100MB must fail");
                } catch (IllegalStateException ise) {
                    //ignore
                }
            }

            {
                // negative value for unbounded buffer
                System.setProperty(BUFFER_MEM_LIMIT_CONFIG_NAME, "-1");

                List<NodeStateEntry> list = Lists.newArrayList(root, e100MB, e1Byte);
                FlatFileStoreIterator fitr = new FlatFileStoreIterator(list.iterator(), ImmutableSet.of());
                NodeState rootNS = fitr.next().getNodeState();
                NodeState aNS = rootNS.getChildNode("a");
                aNS.getChildNode("b");//configure negative value - mem usage limit should be unbounded (long_max)
            }
        } finally {
            if (configuredValue == null) {
                System.clearProperty(BUFFER_MEM_LIMIT_CONFIG_NAME);
            } else {
                System.setProperty(BUFFER_MEM_LIMIT_CONFIG_NAME, configuredValue);
            }
        }
    }
}