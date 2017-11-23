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

import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newExactReclaimer;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

public class ReclaimersTest {

    private static void testOldReclaimer(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(newGCGeneration(3, 3, isCompacted), 2);

        // Don't reclaim young segments
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, true)));

        // Reclaim old and uncompacted segments
        assertTrue(reclaimer.apply(newGCGeneration(1, 3, false)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 3, false)));

        // Don't reclaim old compacted segments from the same full generation
        assertFalse(reclaimer.apply(newGCGeneration(1, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(0, 3, true)));

        // Reclaim old compacted segments from prior full generations
        assertTrue(reclaimer.apply(newGCGeneration(1, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 2, false)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 2, false)));
    }

    private static void testOldReclaimerSequence(boolean isCompacted) {
        Predicate<GCGeneration> reclaimer = newOldReclaimer(newGCGeneration(9, 2, isCompacted), 2);

        assertFalse(reclaimer.apply(newGCGeneration(9, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(9, 2, true)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(8, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(7, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(7, 2, true)));
        assertTrue(reclaimer.apply(newGCGeneration(6, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(6, 2, true)));

        assertTrue(reclaimer.apply(newGCGeneration(5, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(5, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(4, 1, true)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, false)));
        assertTrue(reclaimer.apply(newGCGeneration(3, 1, true)));

        assertTrue(reclaimer.apply(newGCGeneration(2, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(2, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, false)));
        assertTrue(reclaimer.apply(newGCGeneration(1, 0, true)));
        assertTrue(reclaimer.apply(newGCGeneration(0, 0, false)));

    }

    @Test
    public void testOldReclaimerCompactedHead() {
        testOldReclaimer(true);
    }

    @Test
    public void testOldReclaimerUncompactedHead() {
        testOldReclaimer(false);
    }

    @Test
    public void testOldReclaimerSequenceCompactedHead() throws Exception {
        testOldReclaimerSequence(true);
    }

    @Test
    public void testOldReclaimerSequenceUncompactedHead() throws Exception {
        testOldReclaimerSequence(false);
    }

    @Test
    public void testExactReclaimer() {
        Predicate<GCGeneration> reclaimer = newExactReclaimer(newGCGeneration(3, 3, false));
        assertTrue(reclaimer.apply(newGCGeneration(3, 3, false)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 3, true)));
        assertFalse(reclaimer.apply(newGCGeneration(3, 2, false)));
        assertFalse(reclaimer.apply(newGCGeneration(2, 3, false)));
    }
}
