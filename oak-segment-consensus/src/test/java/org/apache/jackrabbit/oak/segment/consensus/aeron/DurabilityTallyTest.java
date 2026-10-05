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
import java.util.Map;

import org.apache.jackrabbit.oak.segment.http.server.util.JsonParser;
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

    @Test
    public void aTallyRestoredFromItsSnapshotEntriesContinuesExactlyLikeTheOriginal() {
        DurabilityTally original = new DurabilityTally(() -> 3);
        original.record("old", 0, true, "h-old", null, 0L);
        original.record("straddling", 0, true, "h-s", null, 10L);
        original.record("failing", 0, false, null, "disk full", 20L);
        original.record("decided", 1, true, "h-d", null, 30L);
        original.record("decided", 2, true, "h-d2", null, 30L);

        List<Map<String, Object>> entries = new ArrayList<>();
        for (Map<String, Object> entry : original.snapshotEntries()) {
            entries.add(JsonParser.parseObject(JsonParser.toJson(entry)));
        }
        DurabilityTally restored = new DurabilityTally(() -> 3);
        restored.record("lost-on-restore", 0, true, "h", null, 5L);
        restored.restore(entries);

        assertFalse(restored.hasReported("lost-on-restore", 0));
        long[] times = {40L, 41L, 42L, RETENTION_MS + 15L, RETENTION_MS + 16L};
        String[] ids = {"straddling", "failing", "decided", "old", "straddling"};
        int[] members = {1, 1, 0, 1, 2};
        boolean[] success = {true, false, true, true, true};
        List<String> expected = continueWith(original, times, ids, members, success);
        assertEquals(List.of("0:straddling:DURABLE:h-s", "1:failing:FAILED:disk full"), expected);
        assertEquals(expected, continueWith(restored, times, ids, members, success));
        for (String id : new String[] {"old", "straddling", "failing", "decided"}) {
            for (int member = 0; member < 3; member++) {
                assertEquals(id + "/" + member, original.hasReported(id, member), restored.hasReported(id, member));
            }
        }
    }

    private static List<String> continueWith(DurabilityTally tally, long[] times, String[] ids, int[] members,
                                             boolean[] success) {
        List<String> decisions = new ArrayList<>();
        for (int i = 0; i < times.length; i++) {
            DurabilityTally.Outcome outcome = tally.record(ids[i], members[i], success[i],
                success[i] ? "h-" + i : null, success[i] ? null : "io", times[i]);
            if (outcome != null) {
                decisions.add(i + ":" + ids[i] + (outcome.success ? ":DURABLE:" + outcome.durableHead
                    : ":FAILED:" + outcome.error));
            }
        }
        return decisions;
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
