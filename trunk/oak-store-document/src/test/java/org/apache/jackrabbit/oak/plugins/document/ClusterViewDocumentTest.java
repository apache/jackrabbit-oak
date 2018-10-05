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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import junitx.util.PrivateAccessor;

public class ClusterViewDocumentTest {

    @Test
    public void testSetToCsv() throws Throwable {
        // test null and empty first
        assertNull(callSetToCsv(null));
        assertNull(callSetToCsv(new HashSet<Integer>()));

        // test one element
        Set<Integer> input = new TreeSet<Integer>();
        StringBuffer sb = new StringBuffer();
        for (int i = 100; i < 199; i++) {
            input.add(i);
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(String.valueOf(i));
            assertEquals(sb.toString(), callSetToCsv(input));
        }
    }

    private String callSetToCsv(Set<Integer> input) throws Throwable {
        return (String) PrivateAccessor.invoke(ClusterViewDocument.class, "setToCsv", new Class[] { Set.class },
                new Object[] { input });
    }

    @Test
    public void testArrayToCsv() throws Throwable {
        // nulls first
        assertNull(callArrayToCsv(null));
        assertNull(callArrayToCsv(new Integer[0]));

        // one element only
        assertEquals("101", callArrayToCsv(new Integer[] { 101 }));
        assertEquals("101,102", callArrayToCsv(new Integer[] { 101, 102 }));
        assertEquals("101,102,103", callArrayToCsv(new Integer[] { 101, 102, 103 }));
        assertEquals("101,102,103,104", callArrayToCsv(new Integer[] { 101, 102, 103, 104 }));
    }

    private String callArrayToCsv(Integer[] input) throws Throwable {
        return (String) PrivateAccessor.invoke(ClusterViewDocument.class, "arrayToCsv", new Class[] { Integer[].class },
                new Object[] { input });
    }

    @Test
    public void testConstructor() {
        ClusterViewDocument doc = new ClusterViewBuilder(1, 3).active(3, 4).asDoc();
        assertNotNull(doc);
        assertEquals(0, doc.getRecoveringIds().size());
        assertEquals(0, doc.getInactiveIds().size());
        assertEquals(2, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(3));
        assertTrue(doc.getActiveIds().contains(4));

        doc = new ClusterViewBuilder(1, 3).active(3, 4).backlogs(5).inactive(5, 6).asDoc();
        assertNotNull(doc);
        assertEquals(0, doc.getRecoveringIds().size());
        assertEquals(2, doc.getInactiveIds().size());
        assertEquals(2, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(3));
        assertTrue(doc.getActiveIds().contains(4));
        assertTrue(doc.getInactiveIds().contains(5));
        assertTrue(doc.getInactiveIds().contains(6));

        doc = new ClusterViewBuilder(11, 4).active(3, 4, 5).recovering(6).inactive(7, 8).asDoc();
        assertNotNull(doc);
        assertEquals(11, doc.getViewSeqNum());
        assertEquals(1, doc.getRecoveringIds().size());
        assertEquals(2, doc.getInactiveIds().size());
        assertEquals(3, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(3));
        assertTrue(doc.getActiveIds().contains(4));
        assertTrue(doc.getActiveIds().contains(5));
        assertTrue(doc.getRecoveringIds().contains(6));
        assertTrue(doc.getInactiveIds().contains(7));
        assertTrue(doc.getInactiveIds().contains(8));

    }
    
    @Test
    public void testHistory() throws Exception {
        final int localClusterId = 1;
        final DocumentNodeStore ns = createMK(localClusterId).nodeStore;

        // initial setting of the view
        final Set<Integer> activeIds = new HashSet<Integer>();
        activeIds.add(localClusterId);
        ClusterViewDocument.readOrUpdate(ns, activeIds, null, null);

        final int LOOP_CNT = 100;
        for (int i = 0; i < LOOP_CNT; i++) {
            // create a new instance
            activeIds.add(i + 2);
            ClusterViewDocument result = ClusterViewDocument.readOrUpdate(ns, activeIds, null, null);
            assertNotNull(result);
            assertEquals(i+2, result.getActiveIds().size());
            if (i<10) {
                assertEquals(i+1, result.getHistory().size());
            } else {
                assertTrue(result.getHistory().size()<=ClusterViewDocument.HISTORY_LIMIT);
            }
        }
    }

    @Test
    public void testReadUpdate() throws Exception {
        final int localClusterId = 11;
        final DocumentNodeStore ns = createMK(localClusterId).nodeStore;

        try {
            ClusterViewDocument.readOrUpdate(ns, null, null, null);
            fail("should complain");
        } catch (Exception ok) {
            // ok
        }

        try {
            ClusterViewDocument.readOrUpdate(ns, new HashSet<Integer>(), null, null);
            fail("should complain");
        } catch (Exception ok) {
            // ok
        }

        Set<Integer> activeIds = new HashSet<Integer>();
        activeIds.add(2);
        Set<Integer> recoveringIds = null;
        Set<Integer> inactiveIds = null;
        // first ever view:
        ClusterViewDocument doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        String createdAt = doc.getCreatedAt();
        assertTrue(createdAt != null && createdAt.length() > 0);
        long createdBy = doc.getCreatedBy();
        assertEquals(localClusterId, createdBy);
        assertEquals(1, doc.getViewSeqNum());
        assertEquals(1, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(2));
        assertEquals(0, doc.getRecoveringIds().size());
        assertEquals(0, doc.getInactiveIds().size());

        // now let's check if it doesn't change anything when we're not doing
        // any update
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(1, doc.getViewSeqNum());

        // and now add a new active id
        activeIds.add(3);
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        createdAt = doc.getCreatedAt();
        assertTrue(createdAt != null && createdAt.length() > 0);
        createdBy = doc.getCreatedBy();
        assertEquals(localClusterId, createdBy);
        assertEquals(2, doc.getViewSeqNum());
        assertEquals(2, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(2));
        assertTrue(doc.getActiveIds().contains(3));
        assertEquals(0, doc.getRecoveringIds().size());
        assertEquals(0, doc.getInactiveIds().size());

        // now let's check if it doesn't change anything when we're not doing
        // any update
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(2, doc.getViewSeqNum());

        // and now add a new recovering id
        recoveringIds = new HashSet<Integer>();
        recoveringIds.add(4);
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        createdAt = doc.getCreatedAt();
        assertTrue(createdAt != null && createdAt.length() > 0);
        createdBy = doc.getCreatedBy();
        assertEquals(localClusterId, createdBy);
        assertEquals(3, doc.getViewSeqNum());
        assertEquals(2, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(2));
        assertTrue(doc.getActiveIds().contains(3));
        assertEquals(1, doc.getRecoveringIds().size());
        assertTrue(doc.getRecoveringIds().contains(4));
        assertEquals(0, doc.getInactiveIds().size());

        // now let's check if it doesn't change anything when we're not doing
        // any update
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(3, doc.getViewSeqNum());

        // and now move that one to inactive
        recoveringIds = new HashSet<Integer>();
        inactiveIds = new HashSet<Integer>();
        inactiveIds.add(4);
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        createdAt = doc.getCreatedAt();
        assertTrue(createdAt != null && createdAt.length() > 0);
        createdBy = doc.getCreatedBy();
        assertEquals(localClusterId, createdBy);
        assertEquals(4, doc.getViewSeqNum());
        assertEquals(2, doc.getActiveIds().size());
        assertTrue(doc.getActiveIds().contains(2));
        assertTrue(doc.getActiveIds().contains(3));
        assertEquals(0, doc.getRecoveringIds().size());
        assertEquals(1, doc.getInactiveIds().size());
        assertTrue(doc.getInactiveIds().contains(4));

        // now let's check if it doesn't change anything when we're not doing
        // any update
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(4, doc.getViewSeqNum());
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(4, doc.getViewSeqNum());
        doc = ClusterViewDocument.readOrUpdate(ns, activeIds, recoveringIds, inactiveIds);
        assertEquals(4, doc.getViewSeqNum());
    }

    private static DocumentMK createMK(int clusterId) {
        return create(new MemoryDocumentStore(), clusterId);
    }

    private static DocumentMK create(DocumentStore ds, int clusterId) {
        return new DocumentMK.Builder().setAsyncDelay(0).setDocumentStore(ds).setClusterId(clusterId)
                .setPersistentCache("target/persistentCache,time").open();
    }

}
