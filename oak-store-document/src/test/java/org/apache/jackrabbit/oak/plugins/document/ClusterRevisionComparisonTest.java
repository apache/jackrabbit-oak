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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ClusterRevisionComparisonTest {
    private MemoryDocumentStore ds = new MemoryDocumentStore();
    private MemoryBlobStore bs = new MemoryBlobStore();
    private Clock clock = new Clock.Virtual();
    private List<DocumentNodeStore> stores = Lists.newArrayList();

    @Before
    public void setUp(){
        Revision.setClock(clock);
    }

    @Test
    public void revisionComparisonMultipleClusterNode() throws Exception{
        DocumentNodeStore c1 = createNS(1);
        DocumentNodeStore c2 = createNS(2);
        DocumentNodeStore c3 = createNS(3);

        //1. Create /a and make it visible to all cluster nodes
        createNode(c1, "/a");
        runBgOps(c1, c2, c3);

        //2. Time T1. Create /a/c2 but do not push the changes yet rT1-C2
        createNode(c2, "/a/c2");

        //3. Time T2. Create /a/c3 and push the changes rT2-C3
        createNode(c3, "/a/c3");
        runBgOps(c3);

        //4. Time T3. Read the changes /a/c3 by c3 created at T2
        // would be considered seen at T3 i.e. rT2-C3 -> rT3-C1
        runBgOps(c1);

        //5. Push changes
        runBgOps(c2);

        //6. Time T4. Read the changes /a/c2 by c2 created at T1.
        // Would be considered seen at T4 i.e. rT1-C2 -> rT4-C1
        // Now from C1 view rT1-C2 > rT2-C3 even though T1 < T2
        //so effectively changes done in future in C3 in absolute time terms
        //is considered to be seen in past by C1
        runBgOps(c1);

        DocumentNodeState c1ns1 = c1.getRoot();
        Iterables.size(c1ns1.getChildNode("a").getChildNodeEntries());

        createNode(c1, "/a/c1");

        //7. Purge revision comparator. Also purge entries from nodeCache
        //such that later reads at rT1-C2 triggers read from underlying DocumentStore
        c1.invalidateNodeCache("/a/c2" , ((DocumentNodeState)c1ns1.getChildNode("a")).getLastRevision());
        c1.invalidateNodeCache("/a/c3" , ((DocumentNodeState)c1ns1.getChildNode("a")).getLastRevision());

        runBgOps(c1);

        NodeState a = c1ns1.getChildNode("a");
        assertTrue("/a/c2 disappeared", a.hasChildNode("c2"));
        assertTrue("/a/c3 disappeared", a.hasChildNode("c3"));

        DocumentNodeState c1ns2 = c1.getRoot();

        //Trigger a diff. With OAK-2144 an exception would be thrown as diff traverses
        //the /a children
        c1ns1.compareAgainstBaseState(c1ns2, new TrackingDiff());
    }

    @Test
    public void revisionComparisonTwoClusterNodes() throws Exception {
        DocumentNodeStore c1 = createNS(1);
        DocumentNodeStore c2 = createNS(2);

        // 1. Create /a and make it visible to both cluster nodes
        createNode(c1, "/a");
        runBgOps(c1, c2);

        // 2. Time T1. Create /a/c2 but do not push the changes yet rT1-C2
        createNode(c2, "/a/c2");

        // 3. Time T2. Create /a/c1
        createNode(c1, "/a/c1");

        // 4. Push changes on c2
        runBgOps(c2);

        // 5. Time T3. Read the changes /a/c2 by c2 created at T1.
        // Now from C1 view rT1-C2 > rT2-C1 even though T1 < T2
        runBgOps(c1);

        DocumentNodeState c1ns1 = c1.getRoot();
        NodeState a = c1ns1.getChildNode("a");
        assertTrue("/a/c1 missing", a.hasChildNode("c1"));
        assertTrue("/a/c2 missing", a.hasChildNode("c2"));

        // 6. Purge revision comparator. Also purge entries from nodeCache
        // such that later reads at rT1-C2 triggers read from underlying DocumentStore
        c1.invalidateNodeCache("/a/c1" , ((DocumentNodeState)a).getLastRevision());
        c1.invalidateNodeCache("/a/c2" , ((DocumentNodeState)a).getLastRevision());

        runBgOps(c1);

        assertTrue("/a/c1 disappeared", a.hasChildNode("c1"));
        assertTrue("/a/c2 disappeared", a.hasChildNode("c2"));

        // read again starting at root node with a invalidated cache
        // and purged revision comparator
        c1ns1 = c1.getRoot();
        c1.invalidateNodeCache("/", c1ns1.getRootRevision());
        c1ns1 = c1.getRoot();
        c1.invalidateNodeCache("/a", c1ns1.getLastRevision());
        assertTrue("/a missing", c1ns1.hasChildNode("a"));
        a = c1ns1.getChildNode("a");
        c1.invalidateNodeCache("/a/c1", ((DocumentNodeState)a).getLastRevision());
        c1.invalidateNodeCache("/a/c2", ((DocumentNodeState)a).getLastRevision());
        assertTrue("/a/c1 disappeared", a.hasChildNode("c1"));
        assertTrue("/a/c2 disappeared", a.hasChildNode("c2"));
    }

    @After
    public void tearDown(){
        for (DocumentNodeStore store : stores) {
            store.dispose();
        }
        stores.clear();
        Revision.resetClockToDefault();
    }

    private DocumentNodeStore createNS(int clusterId){
        DocumentNodeStore store = new DocumentMK.Builder()
                .setDocumentStore(ds)
                .setBlobStore(bs)
                .setClusterId(clusterId)
                .setAsyncDelay(0)
                .open()
                .getNodeStore();
        stores.add(store);
        return store;
    }

   private NodeState createNode(NodeStore ns, String path) throws CommitFailedException {
       NodeBuilder nb = ns.getRoot().builder();
       NodeBuilder cb = nb;
       for (String name : PathUtils.elements(path)) {
           cb = cb.child(name);
       }
       return ns.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
   }

    private static void runBgOps(DocumentNodeStore... stores) {
        for (DocumentNodeStore ns : stores) {
            ns.runBackgroundOperations();
        }
    }

}
