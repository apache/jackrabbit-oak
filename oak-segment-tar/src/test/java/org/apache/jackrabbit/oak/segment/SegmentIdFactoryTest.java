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
package org.apache.jackrabbit.oak.segment;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Set;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;

public class SegmentIdFactoryTest {
    private final MemoryStore store;
    private final SegmentIdProvider idProvider;

    public SegmentIdFactoryTest() throws IOException {
        store = new MemoryStore();
        idProvider = store.getSegmentIdProvider();
    }

    @Test
    public void segmentIdType() {
        assertTrue(idProvider.newDataSegmentId().isDataSegmentId());
        assertTrue(idProvider.newBulkSegmentId().isBulkSegmentId());

        assertFalse(idProvider.newDataSegmentId().isBulkSegmentId());
        assertFalse(idProvider.newBulkSegmentId().isDataSegmentId());
    }

    @Test
    public void internedSegmentIds() {
        assertTrue(idProvider.newSegmentId(0, 0) == idProvider.newSegmentId(0, 0));
        assertTrue(idProvider.newSegmentId(1, 2) == idProvider.newSegmentId(1, 2));
        assertTrue(idProvider.newSegmentId(1, 2) != idProvider.newSegmentId(3, 4));
    }

    @Test
    public void referencedSegmentIds() throws InterruptedException {
        SegmentId a = idProvider.newDataSegmentId();
        SegmentId b = idProvider.newBulkSegmentId();
        SegmentId c = idProvider.newDataSegmentId();

        Set<SegmentId> ids = store.getReferencedSegmentIds();
        assertTrue(ids.contains(a));
        assertTrue(ids.contains(b));
        assertTrue(ids.contains(c));

        // the returned set is a snapshot in time, not continuously updated
        assertFalse(ids.contains(idProvider.newBulkSegmentId()));
    }

    /**
     * This test can't be enabled in general, as gc() contract is too
     * weak for this to work reliably. But it's a good manual check for
     * the correct operation of the tracking of segment id references.
     */
    public void garbageCollection() {
        SegmentId a = idProvider.newDataSegmentId();
        SegmentId b = idProvider.newBulkSegmentId();

        // generate lots of garbage copies of an UUID to get the
        // garbage collector to reclaim also the original instance
        for (int i = 0; i < 1000000; i++) {
            a = new SegmentId(
                    null, a.getMostSignificantBits(), a.getLeastSignificantBits());
        }
        System.gc();

        // now the original UUID should no longer be present
        Set<SegmentId> ids = store.getReferencedSegmentIds();
        assertFalse(ids.contains(a));
        assertTrue(ids.contains(b));
    }

}
