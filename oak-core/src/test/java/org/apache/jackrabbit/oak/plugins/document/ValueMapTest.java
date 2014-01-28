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

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for OAK-1233.
 */
public class ValueMapTest {

    @Test
    public void previousDocs1() {
        String rootId = Utils.getIdFromPath("/");
        Revision r0 = new Revision(0, 0, 1);
        MemoryDocumentStore store = new MemoryDocumentStore();
        // create previous docs
        UpdateOp op = new UpdateOp(Utils.getPreviousIdFor(rootId, r0), true);
        op.set(ID, op.getId());
        op.setMapEntry("prop", r0, "0");
        NodeDocument.setRevision(op, r0, "c");
        store.createOrUpdate(NODES, op);
        Revision r1low = new Revision(1, 0, 1);
        Revision r1high = new Revision(1, 10, 1);
        op = new UpdateOp(Utils.getPreviousIdFor(rootId, r1high), true);
        op.set(ID, op.getId());
        for (int i = r1low.getCounter(); i <= r1high.getCounter(); i++) {
            Revision r = new Revision(1, i, 1);
            op.setMapEntry("foo", r, String.valueOf(i));
            NodeDocument.setRevision(op, r, "c");
        }
        store.createOrUpdate(NODES, op);
        // create root doc
        op = new UpdateOp(rootId, true);
        op.set(ID, op.getId());
        Revision r2 = new Revision(2, 0, 1);
        op.setMapEntry("prop", r2, "1");
        NodeDocument.setRevision(op, r2, "c");
        NodeDocument.setPrevious(op, r0, r0);
        NodeDocument.setPrevious(op, r1high, r1low);
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
        String rootId = Utils.getIdFromPath("/");
        Revision r01 = new Revision(0, 0, 1);
        Revision r12 = new Revision(1, 0, 2);
        Revision r22 = new Revision(2, 0, 2);
        Revision r31 = new Revision(3, 0, 1);
        Revision r42 = new Revision(4, 0, 2);
        Revision r51 = new Revision(5, 0, 1);
        // create previous docs
        UpdateOp op = new UpdateOp(Utils.getPreviousIdFor(rootId, r31), true);
        op.set(ID, op.getId());
        op.setMapEntry("p0", r01, "0");
        NodeDocument.setRevision(op, r01, "c");
        op.setMapEntry("p1", r31, "1");
        NodeDocument.setRevision(op, r31, "c");
        store.createOrUpdate(NODES, op);

        op = new UpdateOp(Utils.getPreviousIdFor(rootId, r42), true);
        op.set(ID, op.getId());
        op.setMapEntry("p1", r12, "0");
        NodeDocument.setRevision(op, r12, "c");
        op.setMapEntry("p1", r22, "1");
        NodeDocument.setRevision(op, r22, "c");
        op.setMapEntry("p0", r42, "1");
        NodeDocument.setRevision(op, r42, "c");
        store.createOrUpdate(NODES, op);

        // create root doc
        op = new UpdateOp(rootId, true);
        op.set(ID, op.getId());
        op.setMapEntry("p0", r51, "2");
        op.setMapEntry("p1", r51, "2");
        NodeDocument.setRevision(op, r51, "c");
        NodeDocument.setPrevious(op, r42, r12);
        NodeDocument.setPrevious(op, r31, r01);
        store.createOrUpdate(NODES, op);

        NodeDocument doc = store.find(NODES, rootId);
        assertNotNull(doc);
        List<NodeDocument> prevDocs = Lists.newArrayList(
                doc.getPreviousDocs("p1", null));
        assertEquals(2, prevDocs.size());
        assertEquals(Utils.getPreviousIdFor(rootId, r31), prevDocs.get(0).getId());
        assertEquals(Utils.getPreviousIdFor(rootId, r42), prevDocs.get(1).getId());

        List<Revision> revs = new ArrayList<Revision>();
        for (Revision r : doc.getValueMap("p1").keySet()) {
            revs.add(r);
        }
        assertEquals(Arrays.asList(r51, r31, r22, r12), revs);
    }
}
