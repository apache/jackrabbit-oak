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
import org.agrona.concurrent.status.AtomicCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Asks Aeron for a cluster snapshot every {@value #INTERVAL_MS_PROPERTY} ms or every
 * {@value #ENTRY_INTERVAL_PROPERTY} applied session messages, whichever comes first (0 disables either).
 *
 * <p>Only the leader toggles {@link ClusterControl.ToggleState#SNAPSHOT}; its consensus module appends a
 * SNAPSHOT action to the log and every member's service snapshots at that log position. Snapshots only bound
 * replay on restart: the archive log is never purged, because snapshots are local to each member and a member
 * with an empty store must still replay the full log (purging would need Oak state transfer).
 */
final class SnapshotTrigger {

    private static final Logger log = LoggerFactory.getLogger(SnapshotTrigger.class);

    static final String INTERVAL_MS_PROPERTY = "oak.aeron.snapshot.intervalMs";
    static final String ENTRY_INTERVAL_PROPERTY = "oak.aeron.snapshot.entryInterval";
    static final long DEFAULT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(10);
    static final long DEFAULT_ENTRY_INTERVAL = 10_000L;

    private final long intervalMs;
    private final long entryInterval;
    private final BooleanSupplier toggle;
    private long entriesSinceSnapshot;
    private long lastSnapshotMs;

    SnapshotTrigger(long intervalMs, long entryInterval, BooleanSupplier toggle, long nowMs) {
        this.intervalMs = intervalMs;
        this.entryInterval = entryInterval;
        this.toggle = toggle;
        this.lastSnapshotMs = nowMs;
    }

    static SnapshotTrigger fromSystemProperties(BooleanSupplier toggle, long nowMs) {
        return new SnapshotTrigger(Long.getLong(INTERVAL_MS_PROPERTY, DEFAULT_INTERVAL_MS),
            Long.getLong(ENTRY_INTERVAL_PROPERTY, DEFAULT_ENTRY_INTERVAL), toggle, nowMs);
    }

    /** Toggles the SNAPSHOT state of the cluster's control counter; false if it is not NEUTRAL or not found. */
    static boolean toggleSnapshot(Cluster cluster) {
        AtomicCounter controlToggle =
            ClusterControl.findControlToggle(cluster.aeron().countersReader(), cluster.context().clusterId());
        return controlToggle != null && ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle);
    }

    /** Called on the service thread after each applied session message. */
    void onEntryApplied(boolean leader, long nowMs) {
        entriesSinceSnapshot++;
        if (!leader || !isDue(nowMs)) {
            return;
        }
        if (toggle.getAsBoolean()) {
            log.info("📸 Requested Aeron snapshot after {} entries / {} ms", entriesSinceSnapshot, nowMs - lastSnapshotMs);
            onSnapshotTaken(nowMs);
        }
    }

    void onSnapshotTaken(long nowMs) {
        entriesSinceSnapshot = 0;
        lastSnapshotMs = nowMs;
    }

    private boolean isDue(long nowMs) {
        return (entryInterval > 0 && entriesSinceSnapshot >= entryInterval)
            || (intervalMs > 0 && nowMs - lastSnapshotMs >= intervalMs);
    }
}
