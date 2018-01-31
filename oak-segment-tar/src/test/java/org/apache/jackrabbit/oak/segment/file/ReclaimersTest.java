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

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.join;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.TAIL;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newExactReclaimer;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

public class ReclaimersTest {

    private static final Map<String, GCGeneration> gcHistory = ImmutableMap.<String, GCGeneration>builder()
        .put("00w", newGCGeneration(0, 0, false))

        // First compaction. Always FULL
        .put("11c", newGCGeneration(1, 1, true))
        .put("11w", newGCGeneration(1, 1, false))

        // TAIL compaction
        .put("21c", newGCGeneration(2, 1, true))
        .put("21w", newGCGeneration(2, 1, false))

        // TAIL compaction
        .put("31c", newGCGeneration(3, 1, true))
        .put("31w", newGCGeneration(3, 1, false))

        // FULL compaction
        .put("42c", newGCGeneration(4, 2, true))
        .put("42w", newGCGeneration(4, 2, false))

        // TAIL compaction
        .put("52c", newGCGeneration(5, 2, true))
        .put("52w", newGCGeneration(5, 2, false))

        // TAIL compaction
        .put("62c", newGCGeneration(6, 2, true))
        .put("62w", newGCGeneration(6, 2, false))

        // FULL compaction
        .put("73c", newGCGeneration(7, 3, true))
        .put("73w", newGCGeneration(7, 3, false))

        .build();

    private static void assertReclaim(Predicate<GCGeneration> reclaimer, String... reclaims) {
        Set<String> toReclaim = newHashSet(reclaims);
        for (Entry<String, GCGeneration> generation : gcHistory.entrySet()) {
            if (reclaimer.apply(generation.getValue())) {
                assertTrue(
                    reclaimer + " should not reclaim " + generation.getKey(),
                    toReclaim.remove(generation.getKey()));
            }
        }

        if (!toReclaim.isEmpty()) {
            fail(reclaimer + " failed to reclaim " + join(",", toReclaim));
        }
    }

    @Test
    public void testOldReclaimer() {
        // 1 retained generation
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(0, 0, false), 1));
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(1, 1, false), 1),
            "00w");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(2, 1, false), 1),
            "00w", "11w");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(3, 1, false), 1),
            "00w", "11w", "21w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(4, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(5, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(6, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "52w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(7, 3, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "42c", "52w", "52c", "62w", "62c");

        // 2 retained generation
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(0, 0, false), 2));
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(1, 1, false), 2));
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(2, 1, false), 2),
            "00w");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(3, 1, false), 2),
            "00w", "11w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(4, 2, false), 2),
            "00w", "11w", "21w");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(5, 2, false), 2),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c");
        assertReclaim(newOldReclaimer(TAIL,
            newGCGeneration(6, 2, false), 2),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(7, 3, false), 2),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "52w");
    }

    @Test
    public void testOldReclaimerDefaultingToFull() {
        // 1 retained generation
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(0, 0, false), 1));
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(1, 1, false), 1),
            "00w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(2, 1, false), 1),
            "00w", "11w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(3, 1, false), 1),
            "00w", "11w", "21w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(4, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(5, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(6, 2, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "52w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(7, 3, false), 1),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "42c", "52w", "52c", "62w", "62c");

        // 2 retained generation
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(0, 0, false), 2));
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(1, 1, false), 2));
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(2, 1, false), 2),
            "00w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(3, 1, false), 2),
            "00w", "11w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(4, 2, false), 2),
            "00w", "11w", "21w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(5, 2, false), 2),
            "00w", "11w", "21w", "31w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(6, 2, false), 2),
            "00w", "11w", "21w", "31w", "42w");
        assertReclaim(newOldReclaimer(FULL,
            newGCGeneration(7, 3, false), 2),
            "00w", "11w", "11c", "21w", "21c", "31w", "31c", "42w", "52w");
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
