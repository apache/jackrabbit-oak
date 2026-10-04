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

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotTriggerTest {

    private final AtomicInteger toggles = new AtomicInteger();

    @Test
    public void followerNeverToggles() {
        SnapshotTrigger trigger = new SnapshotTrigger(1L, 1L, this::toggle, 0L);

        for (int i = 0; i < 5; i++) {
            trigger.onEntryApplied(false, 1_000L * i);
        }

        assertEquals(0, toggles.get());
    }

    @Test
    public void leaderTogglesAfterTheEntryInterval() {
        SnapshotTrigger trigger = new SnapshotTrigger(0L, 3L, this::toggle, 0L);

        trigger.onEntryApplied(true, 1L);
        trigger.onEntryApplied(true, 2L);
        assertEquals(0, toggles.get());
        trigger.onEntryApplied(true, 3L);
        assertEquals(1, toggles.get());
        trigger.onEntryApplied(true, 4L);
        assertEquals(1, toggles.get());
    }

    @Test
    public void leaderTogglesAfterTheTimeInterval() {
        SnapshotTrigger trigger = new SnapshotTrigger(600_000L, 0L, this::toggle, 0L);

        trigger.onEntryApplied(true, 599_999L);
        assertEquals(0, toggles.get());
        trigger.onEntryApplied(true, 600_000L);
        assertEquals(1, toggles.get());
    }

    @Test
    public void aSnapshotOnAnyMemberRestartsTheCount() {
        SnapshotTrigger trigger = new SnapshotTrigger(0L, 2L, this::toggle, 0L);

        trigger.onEntryApplied(false, 1L);
        trigger.onSnapshotTaken(2L);
        trigger.onEntryApplied(true, 3L);

        assertEquals(0, toggles.get());
    }

    @Test
    public void failedToggleIsRetriedOnTheNextEntry() {
        AtomicInteger attempts = new AtomicInteger();
        SnapshotTrigger trigger = new SnapshotTrigger(0L, 1L, () -> attempts.incrementAndGet() > 1, 0L);

        trigger.onEntryApplied(true, 1L);
        trigger.onEntryApplied(true, 2L);

        assertEquals(2, attempts.get());
    }

    @Test
    public void zeroIntervalsDisableTheTrigger() {
        SnapshotTrigger trigger = new SnapshotTrigger(0L, 0L, this::toggle, 0L);

        trigger.onEntryApplied(true, Long.MAX_VALUE);

        assertEquals(0, toggles.get());
    }

    @Test
    public void toggleSnapshotSwitchesTheClusterControlCounterFromNeutral() {
        CountersManager counters = new CountersManager(
            new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024)), new UnsafeBuffer(ByteBuffer.allocateDirect(8 * 1024)));
        int counterId = counters.allocate("control-toggle", ClusterControl.CONTROL_TOGGLE_TYPE_ID,
            key -> key.putInt(0, 42));
        counters.setCounterValue(counterId, ClusterControl.ToggleState.NEUTRAL.code());
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.aeron().countersReader()).thenReturn(counters);
        when(cluster.context().clusterId()).thenReturn(42);

        assertTrue(SnapshotTrigger.toggleSnapshot(cluster));
        assertEquals(ClusterControl.ToggleState.SNAPSHOT.code(), counters.getCounterValue(counterId));
        assertFalse("only from NEUTRAL", SnapshotTrigger.toggleSnapshot(cluster));
    }

    private boolean toggle() {
        toggles.incrementAndGet();
        return true;
    }
}
