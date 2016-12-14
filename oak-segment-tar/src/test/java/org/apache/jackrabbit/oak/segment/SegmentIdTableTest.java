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

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;

public class SegmentIdTableTest {

    private static SegmentIdFactory newSegmentIdMaker(final SegmentStore store) {
        return new SegmentIdFactory() {

            @Nonnull
            @Override
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(store, msb, lsb);
            }

        };
    }

    private static SegmentIdFactory newSegmentIdMaker() throws IOException {
        return newSegmentIdMaker(new MemoryStore());
    }

    /**
     * OAK-2752
     */
    @Test
    public void endlessSearchLoop() throws IOException {
        final SegmentIdFactory maker = newSegmentIdMaker();
        final SegmentIdTable tbl = new SegmentIdTable();

        List<SegmentId> refs = new ArrayList<SegmentId>();
        for (int i = 0; i < 1024; i++) {
            refs.add(tbl.newSegmentId(i, i % 64, maker));
        }

        Callable<SegmentId> c = new Callable<SegmentId>() {

            @Override
            public SegmentId call() throws Exception {
                // (2,1) doesn't exist
                return tbl.newSegmentId(2, 1, maker);
            }
        };
        Future<SegmentId> f = Executors.newSingleThreadExecutor().submit(c);
        SegmentId s = null;
        try {
            s = f.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertNotNull(s);
        assertEquals(2, s.getMostSignificantBits());
        assertEquals(1, s.getLeastSignificantBits());
    }
    
    @Test
    public void randomized() throws IOException {
        SegmentIdFactory maker = newSegmentIdMaker();
        final SegmentIdTable tbl = new SegmentIdTable();

        List<SegmentId> refs = new ArrayList<SegmentId>();
        Random r = new Random(1);
        for (int i = 0; i < 16 * 1024; i++) {
            refs.add(tbl.newSegmentId(r.nextLong(), r.nextLong(), maker));
        }
        assertEquals(16 * 1024, tbl.getEntryCount());
        assertEquals(16 * 2048, tbl.getMapSize());
        assertEquals(5, tbl.getMapRebuildCount());
        
        r = new Random(1);
        for (int i = 0; i < 16 * 1024; i++) {
            refs.add(tbl.newSegmentId(r.nextLong(), r.nextLong(), maker));
            assertEquals(16 * 1024, tbl.getEntryCount());
            assertEquals(16 * 2048, tbl.getMapSize());
            assertEquals(5, tbl.getMapRebuildCount());
        }
    }
    
    @Test
    public void clearTable() throws IOException {
        SegmentIdFactory maker = newSegmentIdMaker();
        final SegmentIdTable tbl = new SegmentIdTable();

        List<SegmentId> refs = new ArrayList<SegmentId>();
        for (int i = 0; i < 8; i++) {
            refs.add(tbl.newSegmentId(i, i % 2, maker));
        }

        Set<UUID> reclaimed = newHashSet();
        for (SegmentId id : refs) {
            if (id.getMostSignificantBits() < 4) {
                reclaimed.add(id.asUUID());
            }
        }

        assertEquals(0, tbl.getMapRebuildCount());

        tbl.clearSegmentIdTables(reclaimed, "TestGcInfo");

        for (SegmentId id : refs) {
            if (id.getMostSignificantBits() < 4) {
                assertEquals("TestGcInfo", id.getGcInfo());
            } else {
                assertNull(id.getGcInfo());
            }
        }
    }
    
    @Test
    public void justHashCollisions() throws IOException {
        SegmentIdFactory maker = newSegmentIdMaker();
        final SegmentIdTable tbl = new SegmentIdTable();

        List<SegmentId> refs = new ArrayList<SegmentId>();
        int originalCount = 1024;
        for (int i = 0; i < originalCount; i++) {
            // modulo 128 to ensure we have conflicts
            refs.add(tbl.newSegmentId(i, i % 128, maker));
        }
        assertEquals(originalCount, tbl.getEntryCount());
        assertEquals(1, tbl.getMapRebuildCount());
        
        List<SegmentId> refs2 = new ArrayList<SegmentId>();
        tbl.collectReferencedIds(refs2);
        assertEquals(refs.size(), refs2.size());

        assertEquals(originalCount, tbl.getEntryCount());
        // we don't expect that there was a refresh, 
        // because there were just hash collisions
        assertEquals(1, tbl.getMapRebuildCount());
    }
    
    @Test
    public void gc() throws IOException {
        SegmentIdFactory maker = newSegmentIdMaker();
        final SegmentIdTable tbl = new SegmentIdTable();

        List<SegmentId> refs = new ArrayList<SegmentId>();
        int originalCount = 1024;
        for (int i = 0; i < originalCount; i++) {
            // modulo 128 to ensure we have conflicts
            refs.add(tbl.newSegmentId(i, i % 128, maker));
        }
        assertEquals(originalCount, tbl.getEntryCount());
        assertEquals(1, tbl.getMapRebuildCount());

        for (int i = 0; i < refs.size() / 2; i++) {
            // we need to remove the first entries,
            // because if we remove the last entries, then
            // getSegmentId would not detect that entries were freed up
            refs.remove(0);
        }
        for (int gcCalls = 0;; gcCalls++) {
            // needed here, so some entries can be garbage collected
            System.gc();
            
            for (SegmentId id : refs) {
                long msb = id.getMostSignificantBits();
                long lsb = id.getLeastSignificantBits();
                SegmentId id2 = tbl.newSegmentId(msb, lsb, maker);
                assertTrue(id2 == id);
            }
            // because we found each entry, we expect the refresh count is the same
            assertEquals(1, tbl.getMapRebuildCount());

            // even thought this does not increase the entry count a lot,
            // it is supposed to detect that entries were removed,
            // and force a refresh, which would get rid of the unreferenced ids
            for (int i = 0; i < 10; i++) {
                tbl.newSegmentId(i, i, maker);
            }

            if (tbl.getEntryCount() < originalCount) {
                break;
            } else if (gcCalls > 10) {
                fail("No entries were garbage collected after 10 times System.gc()");
            }
        }
        assertEquals(2, tbl.getMapRebuildCount());
    }
}