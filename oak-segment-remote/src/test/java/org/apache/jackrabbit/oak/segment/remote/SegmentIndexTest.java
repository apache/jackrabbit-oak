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
package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SegmentIndexTest {

    private static RemoteSegmentArchiveEntry entry(long msb, long lsb, int position) {
        return new RemoteSegmentArchiveEntry(msb, lsb, position, 20, 0, 0, true);
    }

    @Test
    public void getReturnsNullOnEmptyIndex() {
        SegmentIndex index = new SegmentIndex.Builder(0).build();
        assertNull(index.get(1L, 2L));
        assertFalse(index.containsKey(1L, 2L));
        assertEquals(0, index.size());
    }

    @Test
    public void getReturnsPreviouslyPutEntry() {
        SegmentIndex.Builder builder = new SegmentIndex.Builder(1);
        RemoteSegmentArchiveEntry e = entry(0L, 1L, 0);
        builder.put(e);
        SegmentIndex index = builder.build();

        assertSame(e, index.get(0L, 1L));
        assertTrue(index.containsKey(0L, 1L));
    }

    @Test
    public void getReturnsNullForAbsentKeyAmongPresentOnes() {
        SegmentIndex.Builder builder = new SegmentIndex.Builder(3);
        builder.put(entry(0L, 1L, 0));
        builder.put(entry(0L, 2L, 1));
        builder.put(entry(0L, 3L, 2));
        SegmentIndex index = builder.build();

        assertNull(index.get(0L, 99L));
        assertFalse(index.containsKey(0L, 99L));
    }

    @Test
    public void duplicateKeyKeepsGreatestPosition() {
        SegmentIndex.Builder builder = new SegmentIndex.Builder(2);
        RemoteSegmentArchiveEntry older = entry(5L, 6L, 0);
        RemoteSegmentArchiveEntry newer = entry(5L, 6L, 1);
        builder.put(older);
        builder.put(newer);
        SegmentIndex index = builder.build();

        assertSame(newer, index.get(5L, 6L));
        assertEquals(1, index.size());
    }

    @Test
    public void duplicateKeyInsertedOutOfOrderStillKeepsGreatestPosition() {
        SegmentIndex.Builder builder = new SegmentIndex.Builder(2);
        RemoteSegmentArchiveEntry older = entry(5L, 6L, 0);
        RemoteSegmentArchiveEntry newer = entry(5L, 6L, 1);
        builder.put(newer);
        builder.put(older);
        SegmentIndex index = builder.build();

        assertSame(newer, index.get(5L, 6L));
        assertEquals(1, index.size());
    }

    @Test
    public void handlesManyEntriesBeyondInitialCapacity() {
        Random random = new Random(42);
        SegmentIndex.Builder builder = new SegmentIndex.Builder(4);
        Set<RemoteSegmentArchiveEntry> expected = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            RemoteSegmentArchiveEntry e = entry(random.nextLong(), random.nextLong(), i);
            expected.add(e);
            builder.put(e);
        }
        SegmentIndex index = builder.build();

        assertEquals(expected.size(), index.size());
        for (RemoteSegmentArchiveEntry e : expected) {
            assertSame(e, index.get(e.getMsb(), e.getLsb()));
        }
    }

    @Test
    public void valuesReturnsAllSurvivingEntriesWithoutDuplicates() {
        SegmentIndex.Builder builder = new SegmentIndex.Builder(3);
        RemoteSegmentArchiveEntry a = entry(0L, 1L, 0);
        RemoteSegmentArchiveEntry bOld = entry(0L, 2L, 0);
        RemoteSegmentArchiveEntry bNew = entry(0L, 2L, 1);
        builder.put(a);
        builder.put(bOld);
        builder.put(bNew);
        SegmentIndex index = builder.build();

        assertEquals(Set.of(a, bNew), Set.copyOf(index.values()));
    }
}
