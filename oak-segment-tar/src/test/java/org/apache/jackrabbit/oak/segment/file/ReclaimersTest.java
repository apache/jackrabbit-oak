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

import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Test;

public class ReclaimersTest {

    @Test
    public void testOldReclaimer() throws Exception {
        Predicate<GCGeneration> p = newOldReclaimer(newGCGeneration(3, 3, false), 2);

        // Segments from the same full and tail generation should not be
        // removed.

        assertFalse(p.apply(newGCGeneration(3, 3, false)));
        assertFalse(p.apply(newGCGeneration(3, 3, true)));

        // Recent segments, tail or not-tail ones, can't be removed.

        assertFalse(p.apply(newGCGeneration(3, 2, false)));
        assertFalse(p.apply(newGCGeneration(3, 2, true)));

        // Old segments from the same full generation can be removed as long
        // as they are not tail segments.

        assertTrue(p.apply(newGCGeneration(3, 1, false)));
        assertFalse(p.apply(newGCGeneration(3, 1, true)));

        // A full head state from the same full generation can't be removed.

        assertFalse(p.apply(newGCGeneration(3, 0, false)));

        // Some of these generations can be reclaimed, some may not. Since there
        // is a new full head state with full generation 3, every tail and
        // non-tail segment with a tail generation greater than zero can be
        // removed. The full head state with full generation 2 can't be removed,
        // otherwise the condition about the number of retained generations
        // would be violated.

        assertTrue(p.apply(newGCGeneration(2, 2, false)));
        assertTrue(p.apply(newGCGeneration(2, 2, true)));
        assertTrue(p.apply(newGCGeneration(2, 1, false)));
        assertTrue(p.apply(newGCGeneration(2, 1, true)));
        assertFalse(p.apply(newGCGeneration(2, 0, false)));

        // These generations can be reclaimed because their full generation is
        // too old when compared to the number of retained generations.

        assertTrue(p.apply(newGCGeneration(1, 2, false)));
        assertTrue(p.apply(newGCGeneration(1, 2, true)));
        assertTrue(p.apply(newGCGeneration(1, 1, false)));
        assertTrue(p.apply(newGCGeneration(1, 1, true)));
        assertTrue(p.apply(newGCGeneration(1, 0, false)));
    }

}
