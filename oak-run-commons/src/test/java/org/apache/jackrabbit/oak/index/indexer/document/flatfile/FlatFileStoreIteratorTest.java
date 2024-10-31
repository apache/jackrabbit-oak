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

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
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

    private static final File TEST_FOLDER = new File("target", "test");

    protected FlatFileStoreIterator newInMemoryFlatFileStore(
            Iterator<NodeStateEntry> it, Set<String> set, int memMB) {
        BlobStore blobStore = null;
        return new FlatFileStoreIterator(blobStore, TEST_FOLDER.getPath(),  it, set, memMB);
    }

    protected FlatFileStoreIterator newFlatFileStore(Iterator<NodeStateEntry> it, Set<String> set) {
        BlobStore blobStore = null;
        return new FlatFileStoreIterator(blobStore, TEST_FOLDER.getPath(),  it, set);
    }

    @Test
    public void simpleTraversal() {
        Set<String> preferred = Set.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e", "/e/e"));

        try (FlatFileStoreIterator fitr = newFlatFileStore(citr.iterator(), preferred)) {
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
            assertEquals(1, nse4.getNodeState().getChildNodeCount(100));

            NodeStateEntry nse5 = fitr.next();
            assertEquals("/e/e", nse5.getPath());
            assertEquals(0, nse5.getNodeState().getChildNodeCount(100));

            assertFalse(fitr.hasNext());
        }
    }

    @Test
    public void invalidOrderAccess() {
        Set<String> preferred = Set.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e"));

        try (FlatFileStoreIterator fitr = newFlatFileStore(citr.iterator(), preferred)) {
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
    }

    // OAK-7284
    @Test
    public void comodificationException() {
        Set<String> preferred = Set.of("j:c");

        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/j:c", "/a/j:c/j:c", "/a/b"));

        try (FlatFileStoreIterator fitr = newFlatFileStore(citr.iterator(), preferred)) {
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
    }

    // OAK-7285
    @Test
    public void getChildNodeLimitedByNonPreferred() {
        // have more than 1 preferred names
        Set<String> preferred = Set.of("j:c", "md");

        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/b", "/a/c"));

        try (FlatFileStoreIterator fitr = newFlatFileStore(citr.iterator(), preferred)) {

            NodeStateEntry a = fitr.next();
            assertEquals("/a", a.getPath());

            NodeState aNS = a.getNodeState();
            aNS.getChildNode("j:c");

            // Don't read whole tree to conclude that "j:c" doesn't exist (reading /a/b should imply that it doesn't exist)
            assertEquals(1, fitr.getBufferSize());

            // read remaining entries to trigger release of resources
            Iterators.size(fitr);
        }
    }

    @Test
    public void bufferEstimatesMemory() {
        List<NodeStateEntry> nseList = List.of(
                new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/a").withMemUsage(20).build(),
                new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/a/b").withMemUsage(30).build()
        );
        FlatFileStoreIterator fitr = newInMemoryFlatFileStore(nseList.iterator(), Set.of(), 100);

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
    public void memUsageConfig100() {
        try {
            NodeStateEntry root = new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/").build();
            NodeStateEntry e1Byte = new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/a/b").withMemUsage(1).build();
            NodeStateEntry e1MB = new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/a").withMemUsage(1 * 1024 * 1024).build();
            NodeStateEntry e100MB = new NodeStateEntryBuilder(EmptyNodeState.EMPTY_NODE, "/a").withMemUsage(100 * 1024 * 1024).build();

            {
                // 100 MB limit
                int mb = 100;
                List<NodeStateEntry> list = List.of(root, e100MB, e1Byte);
                FlatFileStoreIterator fitr = newInMemoryFlatFileStore(list.iterator(), Set.of(), mb);
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
                int mb = 1;
                System.setProperty(BUFFER_MEM_LIMIT_CONFIG_NAME, "1");

                List<NodeStateEntry> list = List.of(root, e1MB, e1Byte);
                FlatFileStoreIterator fitr = newInMemoryFlatFileStore(list.iterator(), Set.of(), mb);
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
                // negative value for unbounded buffer
                int mb = -1;

                List<NodeStateEntry> list = List.of(root, e100MB, e1Byte);
                FlatFileStoreIterator fitr = newInMemoryFlatFileStore(list.iterator(), Set.of(), mb);
                NodeState rootNS = fitr.next().getNodeState();
                NodeState aNS = rootNS.getChildNode("a");
                aNS.getChildNode("b");//configure negative value - mem usage limit should be unbounded (long_max)
            }
        } finally {
            // ignore
        }
    }
}
