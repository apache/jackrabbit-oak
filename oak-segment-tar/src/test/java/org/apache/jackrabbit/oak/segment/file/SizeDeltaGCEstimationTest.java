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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SizeDeltaGCEstimationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private GCJournal journal;

    @Before
    public void setUpJournal() throws Exception {
        journal = new GCJournal(folder.getRoot());
    }

    @Test
    public void testFullSkippedEstimation() {
        assertTrue(new SizeDeltaGcEstimation(0, journal, 1000, true).estimate().isGcNeeded());
    }

    @Test
    public void testTailSkippedEstimation() {
        assertTrue(new SizeDeltaGcEstimation(0, journal, 1000, false).estimate().isGcNeeded());
    }

    @Test
    public void testFullEmptyJournal() {
        assertTrue(new SizeDeltaGcEstimation(100, journal, 1000, true).estimate().isGcNeeded());
    }

    @Test
    public void testTailEmptyJournal() {
        assertTrue(new SizeDeltaGcEstimation(100, journal, 1000, false).estimate().isGcNeeded());
    }

    @Test
    public void testTailGCNeeded() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 1, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 1, true), 1000, "id");
        assertTrue(new SizeDeltaGcEstimation(50, journal, 1300, false).estimate().isGcNeeded());
    }

    @Test
    public void testTailGCSkipped() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 1, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 1, true), 1000, "id");
        assertFalse(new SizeDeltaGcEstimation(200, journal, 1300, false).estimate().isGcNeeded());
    }

    @Test
    public void testFullGCNeededBecauseOfSize1() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 2, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 3, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 4, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 5, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 6, true), 1000, "id");
        assertTrue(new SizeDeltaGcEstimation(100, journal, 1300, true).estimate().isGcNeeded());
    }

    @Test
    public void testFullGCNeededBecauseOfSize2() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        assertTrue(new SizeDeltaGcEstimation(10, journal, 1030, true).estimate().isGcNeeded());
    }

    @Test
    public void testFullGCSkippedBecauseOfSize1() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 2, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 3, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 4, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 5, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 6, true), 1000, "id");
        assertFalse(new SizeDeltaGcEstimation(100, journal, 1030, true).estimate().isGcNeeded());
    }

    @Test
    public void testFullGCSkippedBecauseOfSize2() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        assertFalse(new SizeDeltaGcEstimation(100, journal, 1030, true).estimate().isGcNeeded());
    }

    @Test
    public void testFullGCNeededBecauseOfPreviousTail() throws Exception {
        journal.persist(100, 1000, newGCGeneration(1, 1, true), 1000, "id");
        journal.persist(110, 1100, newGCGeneration(2, 1, true), 1000, "id");
        journal.persist(120, 1200, newGCGeneration(3, 1, true), 1000, "id");
        journal.persist(130, 1000, newGCGeneration(4, 2, true), 1000, "id");
        journal.persist(100, 1010, newGCGeneration(5, 2, true), 1000, "id");
        journal.persist(110, 1020, newGCGeneration(6, 2, true), 1000, "id");
        assertTrue(new SizeDeltaGcEstimation(100, journal, 1030, true).estimate().isGcNeeded());
    }

}
