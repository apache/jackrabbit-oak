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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for OAK-1233.
 */
public class ValueMapTest {

    @Test
    public void previousDocs1() {
        String rootPath = "/";
        String rootId = Utils.getIdFromPath(rootPath);
        Revision r0 = new Revision(0, 0, 1);
        MemoryDocumentStore store = new MemoryDocumentStore();
        // create previous docs
        UpdateOp op = new UpdateOp(Utils.getPreviousIdFor(rootPath, r0, 0), true);
        op.setMapEntry("prop", r0, "0");
        NodeDocument.setRevision(op, r0, "c");
        store.createOrUpdate(NODES, op);
        Revision r1low = new Revision(1, 0, 1);
        Revision r1high = new Revision(1, 10, 1);
        op = new UpdateOp(Utils.getPreviousIdFor(rootPath, r1high, 0), true);
        for (int i = r1low.getCounter(); i <= r1high.getCounter(); i++) {
            Revision r = new Revision(1, i, 1);
            op.setMapEntry("foo", r, String.valueOf(i));
            NodeDocument.setRevision(op, r, "c");
        }
        store.createOrUpdate(NODES, op);
        // create root doc
        op = new UpdateOp(rootId, true);
        Revision r2 = new Revision(2, 0, 1);
        op.setMapEntry("prop", r2, "1");
        NodeDocument.setRevision(op, r2, "c");
        NodeDocument.setPrevious(op, new Range(r0, r0, 0));
        NodeDocument.setPrevious(op, new Range(r1high, r1low, 0));
        store.createOrUpdate(NODES, op);

        NodeDocument doc = store.find(NODES, rootId);
        assertNotNull(doc);
        Set<Revision> revs = doc.getValueMap("prop").keySet();
        assertEquals(2, revs.size());
        assertTrue(revs.contains(r0));
        assertTrue(revs.contains(r2));
        Iterator<Revision> it = revs.iterator();
        assertTrue(it.hasNext());
        assertEquals(r2, it.next());
        assertTrue(it.hasNext());
        assertEquals(r0, it.next());
    }

    @Test
    public void previousDocs2() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        String rootPath = "/";
        String rootId = Utils.getIdFromPath(rootPath);
        Revision r01 = new Revision(0, 0, 1);
        Revision r12 = new Revision(1, 0, 2);
        Revision r22 = new Revision(2, 0, 2);
        Revision r31 = new Revision(3, 0, 1);
        Revision r42 = new Revision(4, 0, 2);
        Revision r51 = new Revision(5, 0, 1);
        // create previous docs
        UpdateOp op = new UpdateOp(Utils.getPreviousIdFor(rootPath, r31, 0), true);
        op.setMapEntry("p0", r01, "0");
        NodeDocument.setRevision(op, r01, "c");
        op.setMapEntry("p1", r31, "1");
        NodeDocument.setRevision(op, r31, "c");
        store.createOrUpdate(NODES, op);

        op = new UpdateOp(Utils.getPreviousIdFor(rootPath, r42, 0), true);
        op.setMapEntry("p1", r12, "0");
        NodeDocument.setRevision(op, r12, "c");
        op.setMapEntry("p1", r22, "1");
        NodeDocument.setRevision(op, r22, "c");
        op.setMapEntry("p0", r42, "1");
        NodeDocument.setRevision(op, r42, "c");
        store.createOrUpdate(NODES, op);

        // create root doc
        op = new UpdateOp(rootId, true);
        op.setMapEntry("p0", r51, "2");
        op.setMapEntry("p1", r51, "2");
        NodeDocument.setRevision(op, r51, "c");
        NodeDocument.setPrevious(op, new Range(r42, r12, 0));
        NodeDocument.setPrevious(op, new Range(r31, r01, 0));
        store.createOrUpdate(NODES, op);

        NodeDocument doc = store.find(NODES, rootId);
        assertNotNull(doc);
        List<NodeDocument> prevDocs = Lists.newArrayList(
                doc.getPreviousDocs("p1", null));
        assertEquals(2, prevDocs.size());
        assertEquals(Utils.getPreviousIdFor(rootPath, r31, 0), prevDocs.get(0).getId());
        assertEquals(Utils.getPreviousIdFor(rootPath, r42, 0), prevDocs.get(1).getId());

        List<Revision> revs = new ArrayList<Revision>();
        for (Revision r : doc.getValueMap("p1").keySet()) {
            revs.add(r);
        }
        assertEquals(Arrays.asList(r51, r31, r22, r12), revs);
    }
    
    // OAK-2433
    @Test
    public void mergeSorted() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder().setAsyncDelay(0).getNodeStore();
        DocumentStore docStore = store.getDocumentStore();
        String id = Utils.getIdFromPath("/");
        
        List<NodeBuilder> branches = Lists.newArrayList();
        int i = 0;
        while (docStore.find(NODES, id).getPreviousRanges().size() < 2) {
            i++;
            NodeBuilder builder = store.getRoot().builder();
            builder.child("foo").setProperty("prop", i);
            builder.child("bar").setProperty("prop", i);
            if (i % 7 == 0) {
                // every now and then create a branch
                int numRevs = docStore.find(NODES, id).getLocalRevisions().size();
                NodeBuilder child = builder.child("node" + i);
                int p = 0;
                while (numRevs == docStore.find(NODES, id).getLocalRevisions().size()) {
                    child.setProperty("prop", p++);
                }
                branches.add(builder);
            } else {
                store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            store.runBackgroundOperations();
        }
        
        NodeDocument doc = docStore.find(NODES, id);
        Iterators.size(doc.getValueMap(NodeDocument.REVISIONS).entrySet().iterator());

        store.dispose();
    }

    // OAK-2433
    @Test
    public void mergeSorted1() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        
        Revision r1 = new Revision(1, 0, 1); // prev2
        Revision r2 = new Revision(2, 0, 1); // prev2
        Revision r3 = new Revision(3, 0, 1); // root
        Revision r4 = new Revision(4, 0, 1); // prev2
        Revision r5 = new Revision(5, 0, 1); // prev1
        Revision r6 = new Revision(6, 0, 1); // root
        Revision r7 = new Revision(7, 0, 1); // prev1

        Range range1 = new Range(r7, r5, 0);
        Range range2 = new Range(r4, r1, 0);
        
        String prevId1 = Utils.getPreviousIdFor("/", range1.high, 0);
        UpdateOp prevOp1 = new UpdateOp(prevId1, true);
        NodeDocument.setRevision(prevOp1, r5, "c");
        NodeDocument.setRevision(prevOp1, r7, "c");

        String prevId2 = Utils.getPreviousIdFor("/", range2.high, 0);
        UpdateOp prevOp2 = new UpdateOp(prevId2, true);
        NodeDocument.setRevision(prevOp2, r1, "c");
        NodeDocument.setRevision(prevOp2, r2, "c");
        NodeDocument.setRevision(prevOp2, r4, "c");

        String rootId = Utils.getIdFromPath("/");
        UpdateOp op = new UpdateOp(rootId, true);
        NodeDocument.setRevision(op, r3, "c");
        NodeDocument.setRevision(op, r6, "c");
        NodeDocument.setPrevious(op, range1);
        NodeDocument.setPrevious(op, range2);
        
        store.create(NODES, Lists.newArrayList(op, prevOp1, prevOp2));
        
        NodeDocument doc = store.find(NODES, rootId);
        Iterators.size(doc.getValueMap(NodeDocument.REVISIONS).entrySet().iterator());
    }
}
