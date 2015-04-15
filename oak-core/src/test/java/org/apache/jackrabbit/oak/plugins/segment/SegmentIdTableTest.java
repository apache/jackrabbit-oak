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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import junit.framework.Assert;

import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class SegmentIdTableTest {

    /**
     * OAK-2752
     */
    @Test
    public void endlessSearchLoop() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        for (int i = 0; i < 1024; i++) {
            refs.add(tbl.getSegmentId(i, i % 64));
        }

        Callable<SegmentId> c = new Callable<SegmentId>() {

            @Override
            public SegmentId call() throws Exception {
                // (2,1) doesn't exist
                return tbl.getSegmentId(2, 1);
            }
        };
        Future<SegmentId> f = Executors.newSingleThreadExecutor().submit(c);
        SegmentId s = null;
        try {
            s = f.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotNull(s);
        Assert.assertEquals(2, s.getMostSignificantBits());
        Assert.assertEquals(1, s.getLeastSignificantBits());
    }
    
    @Test
    public void randomized() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        Random r = new Random(1);
        for (int i = 0; i < 16 * 1024; i++) {
            refs.add(tbl.getSegmentId(r.nextLong(), r.nextLong()));
        }
        Assert.assertEquals(16 * 1024, tbl.getEntryCount());
        Assert.assertEquals(16 * 2048, tbl.getMapSize());
        Assert.assertEquals(5, tbl.getMapRebuildCount());
        
        r = new Random(1);
        for (int i = 0; i < 16 * 1024; i++) {
            refs.add(tbl.getSegmentId(r.nextLong(), r.nextLong()));
            Assert.assertEquals(16 * 1024, tbl.getEntryCount());
            Assert.assertEquals(16 * 2048, tbl.getMapSize());
            Assert.assertEquals(5, tbl.getMapRebuildCount());
        }
    }
    
    @Test
    public void clearTable() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        int originalCount = 8;
        for (int i = 0; i < originalCount; i++) {
            refs.add(tbl.getSegmentId(i, i % 2));
        }
        Assert.assertEquals(originalCount, tbl.getEntryCount());
        Assert.assertEquals(0, tbl.getMapRebuildCount());
        
        tbl.clearSegmentIdTables(new CompactionStrategy(false, false, 
                CleanupType.CLEAN_NONE, originalCount, (byte) 0) {

            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead)
                    throws Exception {
                return true;
            }

            @Override
            public boolean canRemove(SegmentId id) {
                return id.getMostSignificantBits() < 4;
            }
            
        });
        
        Assert.assertEquals(4, tbl.getEntryCount());

        for (SegmentId id : refs) {
            if (id.getMostSignificantBits() >= 4) {
                SegmentId id2 = tbl.getSegmentId(
                        id.getMostSignificantBits(),
                        id.getLeastSignificantBits());
                List<SegmentId> list = tbl.getRawSegmentIdList();
                if (list.size() != new HashSet<SegmentId>(list).size()) {
                    Collections.sort(list);
                    fail("duplicate entry " + list.toString());
                }
                Assert.assertTrue(id == id2);
            }
        }
    }
    
    @Test
    public void justHashCollisions() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        int originalCount = 1024;
        for (int i = 0; i < originalCount; i++) {
            // modulo 128 to ensure we have conflicts
            refs.add(tbl.getSegmentId(i, i % 128));
        }
        Assert.assertEquals(originalCount, tbl.getEntryCount());
        Assert.assertEquals(1, tbl.getMapRebuildCount());
        
        List<SegmentId> refs2 = new ArrayList<SegmentId>();
        tbl.collectReferencedIds(refs2);
        Assert.assertEquals(refs.size(), refs2.size());

        Assert.assertEquals(originalCount, tbl.getEntryCount());
        // we don't expect that there was a refresh, 
        // because there were just hash collisions
        Assert.assertEquals(1, tbl.getMapRebuildCount());
    }
    
    @Test
    public void gc() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        int originalCount = 1024;
        for (int i = 0; i < originalCount; i++) {
            // modulo 128 to ensure we have conflicts
            refs.add(tbl.getSegmentId(i, i % 128));
        }
        Assert.assertEquals(originalCount, tbl.getEntryCount());
        Assert.assertEquals(1, tbl.getMapRebuildCount());

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
                SegmentId id2 = tbl.getSegmentId(id.getMostSignificantBits(), id.getLeastSignificantBits());
                Assert.assertTrue(id2 == id);
            }
            // because we found each entry, we expect the refresh count is the same
            Assert.assertEquals(1, tbl.getMapRebuildCount());

            // even thought this does not increase the entry count a lot,
            // it is supposed to detect that entries were removed,
            // and force a refresh, which would get rid of the unreferenced ids
            for (int i = 0; i < 10; i++) {
                tbl.getSegmentId(i, i);
            }

            if (tbl.getEntryCount() < originalCount) {
                break;
            } else if (gcCalls > 10) {
                fail("No entries were garbage collected after 10 times System.gc()");
            }
        }
        Assert.assertEquals(2, tbl.getMapRebuildCount());
    }
}