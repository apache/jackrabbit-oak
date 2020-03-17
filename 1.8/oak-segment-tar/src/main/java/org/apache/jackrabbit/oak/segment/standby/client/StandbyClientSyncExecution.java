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

package org.apache.jackrabbit.oak.segment.standby.client;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the algorithm for a single execution of the synchronization
 * process between the primary and the standby instance. It also contains
 * temporary state that is supposed to be used for the lifetime of a
 * synchronization run.
 */
class StandbyClientSyncExecution {

    private static final Logger log = LoggerFactory.getLogger(StandbyClientSyncExecution.class);

    private final FileStore store;

    private final SegmentIdProvider idProvider;

    private final Supplier<Boolean> running;

    StandbyClientSyncExecution(FileStore store, Supplier<Boolean> running) {
        this.store = store;
        this.idProvider = store.getSegmentIdProvider();
        this.running = running;
    }

    void execute(StandbyClient client) throws Exception {
        RecordId remoteHead = getHead(client);

        if (remoteHead == null) {
            log.error("Unable to fetch remote head");
            return;
        }

        if (remoteHead.equals(store.getHead().getRecordId())) {
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        SegmentNodeState before = store.getHead();
        SegmentNodeBuilder builder = before.builder();
        SegmentNodeState current = newSegmentNodeState(remoteHead);
        compareAgainstBaseState(client, current, before, builder);
        store.getRevisions().setHead(before.getRecordId(), remoteHead);
        log.info("Updated head state in {}", stopwatch);
    }

    @Nullable
    private RecordId getHead(StandbyClient client) throws Exception {
        String head = client.getHead();
        if (head == null) {
            return null;
        }
        return RecordId.fromString(idProvider, head);
    }

    private SegmentNodeState newSegmentNodeState(RecordId id) {
        return store.getReader().readNode(id);
    }

    private void compareAgainstBaseState(StandbyClient client, SegmentNodeState current, SegmentNodeState before, SegmentNodeBuilder builder) throws Exception {
        while (true) {
            try {
                current.compareAgainstBaseState(before, new StandbyDiff(builder, store, client, running));
                return;
            } catch (SegmentNotFoundException e) {
                log.debug("Found missing segment {}", e.getSegmentId());
                copySegmentHierarchyFromPrimary(client, UUID.fromString(e.getSegmentId()));
            }
        }
    }

    private void copySegmentHierarchyFromPrimary(StandbyClient client, UUID segmentId) throws Exception {
        Set<UUID> visited = new HashSet<>();
        List<UUID> bulk = new LinkedList<>();
        List<UUID> data = new LinkedList<>();

        deriveTopologicalOrder(client, segmentId, visited, data, bulk);

        for (UUID id : bulk) {
            log.info("Copying bulk segment {} from primary", id);
            copySegmentFromPrimary(client, id);
        }

        for (UUID id : data) {
            log.info("Copying data segment {} from primary", id);
            copySegmentFromPrimary(client, id);
        }
    }

    private void deriveTopologicalOrder(StandbyClient client, UUID id, Set<UUID> visited, List<UUID> data, List<UUID> bulk) throws Exception {
        if (visited.contains(id) || isLocal(id)) {
            return;
        }

        // Use DFS to traverse segment graph and make sure
        // to add each data segment to the data list only
        // after all its references were already added

        log.debug("Inspecting segment {}", id);
        visited.add(id);

        if (SegmentId.isDataSegmentId(id.getLeastSignificantBits())) {
            for (String s : readReferences(client, id)) {
                UUID referenced = UUID.fromString(s);
                log.debug("Found reference from {} to {}", id, referenced);
                deriveTopologicalOrder(client, referenced, visited, data, bulk);
            }

            data.add(id);
        } else {
            bulk.add(id);
        }
    }

    private Iterable<String> readReferences(StandbyClient client, UUID id) throws InterruptedException {
        Iterable<String> references = client.getReferences(id.toString());

        if (references == null) {
            throw new IllegalStateException(String.format("Unable to read references of segment %s from primary", id));
        }

        return references;
    }

    private boolean isLocal(UUID id) {
        return store.containsSegment(idProvider.newSegmentId(
                id.getMostSignificantBits(),
                id.getLeastSignificantBits()
        ));
    }

    private void copySegmentFromPrimary(StandbyClient client, UUID uuid) throws Exception {
        byte[] data = client.getSegment(uuid.toString());

        if (data == null) {
            throw new IllegalStateException("Unable to read segment " + uuid);
        }

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        SegmentId segmentId = idProvider.newSegmentId(msb, lsb);
        store.writeSegment(segmentId, data, 0, data.length);
    }

}

