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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.jackrabbit.oak.segment.file.EstimationStrategy.Context;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FullSizeDeltaEstimationStrategyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private GCJournal journal;

    @Before
    public void setUpJournal() {
        journal = new GCJournal(new TarPersistence(folder.getRoot()).getGCJournalFile());
    }

    @Test
    public void testFullSkippedEstimation() {
        assertTrue(isGarbageCollectionNeeded(0, 1000));
    }

    @Test
    public void testFullEmptyJournal() {
        assertTrue(isGarbageCollectionNeeded(100, 1000));
    }

    @Test
    public void testFullGCNeededBecauseOfSize1() {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 2, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 3, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 4, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 5, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 6, true), 1000, "id");
        assertTrue(isGarbageCollectionNeeded(100, 1300));
    }

    @Test
    public void testFullGCNeededBecauseOfSize2() {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        assertTrue(isGarbageCollectionNeeded(10, 1030));
    }

    @Test
    public void testFullGCSkippedBecauseOfSize1() {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 2, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 3, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 4, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 5, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 6, true), 1000, "id");
        assertFalse(isGarbageCollectionNeeded(100, 1030));
    }

    @Test
    public void testFullGCSkippedBecauseOfSize2() {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        assertFalse(isGarbageCollectionNeeded(100, 1030));
    }

    @Test
    public void testFullGCNeededBecauseOfPreviousTail() {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 1, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 1, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 2, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 2, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 2, true), 1000, "id");
        assertTrue(isGarbageCollectionNeeded(100, 1030));
    }

    private boolean isGarbageCollectionNeeded(long delta, long size) {
        return new FullSizeDeltaEstimationStrategy().estimate(new Context() {

            @Override
            public long getSizeDelta() {
                return delta;
            }

            @Override
            public long getCurrentSize() {
                return size;
            }

            @Override
            public GCJournal getGCJournal() {
                return journal;
            }

        }).isGcNeeded();
    }

}
