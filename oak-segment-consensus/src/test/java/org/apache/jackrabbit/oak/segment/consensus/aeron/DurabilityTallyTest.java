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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.apache.jackrabbit.oak.segment.consensus.aeron.DurabilityTally.RETENTION_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DurabilityTallyTest {

    @Test
    public void aBurstIsForgottenOnceItsRetentionWindowOfLogTimeHasPassed() {
        DurabilityTally tally = new DurabilityTally(() -> 3);
        int burst = 60_000;
        for (int i = 0; i < burst; i++) {
            tally.record("p" + i, 0, true, "h", null, i);
            assertNotNull(tally.record("p" + i, 1, true, "h", null, i));
        }
        assertTrue(tally.hasReported("p0", 2));

        tally.record("next", 0, true, "h", null, burst - 1 + RETENTION_MS);

        for (int i = 0; i < burst; i++) {
            assertFalse("p" + i, tally.hasReported("p" + i, 2));
        }
    }

    @Test
    public void aLateReportWithinTheWindowIsAbsorbed() {
        DurabilityTally tally = new DurabilityTally(() -> 3);
        tally.record("p", 0, true, "h", null, 0L);
        tally.record("p", 1, true, "h", null, 0L);

        assertNull(tally.record("p", 2, true, "h", null, RETENTION_MS - 1));
        assertTrue(tally.hasReported("p", 2));
    }

    @Test
    public void undecidedReportsAreForgottenAfterTheSameWindow() {
        DurabilityTally tally = new DurabilityTally(() -> 3);
        tally.record("p", 0, true, "h", null, 0L);

        tally.record("q", 0, true, "h", null, RETENTION_MS - 1);
        assertTrue(tally.hasReported("p", 0));

        tally.record("q", 1, true, "h", null, RETENTION_MS);
        assertFalse(tally.hasReported("p", 0));
    }

    @Test
    public void membersForgetAndDecideAtTheSameLogEntries() {
        long[] times = {0L, 5L, RETENTION_MS, RETENTION_MS + 1, RETENTION_MS + 2, 2 * RETENTION_MS + 5};
        String[] ids = {"p", "q", "p", "q", "p", "q"};
        int[] members = {0, 0, 1, 1, 2, 2};

        List<String> first = apply(times, ids, members);
        assertEquals(List.of("3:q", "4:p"), first);
        assertEquals(first, apply(times, ids, members));
    }

    private static List<String> apply(long[] times, String[] ids, int[] members) {
        DurabilityTally tally = new DurabilityTally(() -> 3);
        List<String> decisions = new ArrayList<>();
        for (int i = 0; i < times.length; i++) {
            if (tally.record(ids[i], members[i], true, "h", null, times[i]) != null) {
                decisions.add(i + ":" + ids[i]);
            }
        }
        return decisions;
    }
}
