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
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
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
        LinkedList<UUID> batch = new LinkedList<>();

        batch.offer(segmentId);

        LinkedList<UUID> bulk = new LinkedList<>();
        LinkedList<UUID> data = new LinkedList<>();

        Set<UUID> visited = new HashSet<>();
        Set<UUID> queued = new HashSet<>();
        Set<UUID> local = new HashSet<>();

        while (!batch.isEmpty()) {
            UUID current = batch.remove();

            log.debug("Inspecting segment {}", current);
            visited.add(current);

            // Add the current segment ID at the beginning of the respective
            // list, depending on its type. This allows to process those
            // segments in an optimal topological order later on. If the current
            // segment is a bulk segment, we can skip the rest of the loop,
            // since bulk segments don't reference any other segment.

            if (SegmentId.isDataSegmentId(current.getLeastSignificantBits())) {
                data.addFirst(current);
            } else {
                bulk.addFirst(current);
                continue;
            }

            for (String s : readReferences(client, current)) {
                UUID referenced = UUID.fromString(s);

                // Short circuit for the "backward reference". The segment graph
                // is not guaranteed to be acyclic, so there might be segments
                // pointing back to a previously visited (but locally
                // unavailable) segment.

                if (visited.contains(referenced)) {
                    continue;
                }

                // Short circuit for the "diamond problem". Imagine that segment
                // S1 references S2 and S3 and both S2 and S3 reference S4.
                // These references form the shape of a diamond. If the segments
                // are processed in the order S1, S2, S3, then S4 is added twice
                // to the 'batch' queue. The following check prevents processing
                // S4 twice or more.

                if (queued.contains(referenced)) {
                    continue;
                }

                // Short circuit for the "sharing-is-caring problem". If many
                // new segments are sharing segments that are already locally
                // available, we should not issue a request for it to the
                // server. Moreover, if a segment was visited and persisted
                // during this synchronization process, it will end up in the
                // 'local' set as well.

                if (local.contains(referenced)) {
                    continue;
                }

                if (isLocal(referenced)) {
                    local.add(referenced);
                    continue;
                }

                // If we arrive at this point, the referenced segment is 1) not
                // present locally, 2) not already queued for retrieval and 3)
                // never visited before. We can safely add the reference to the
                // queue and transfer the segment later.

                log.debug("Found reference from {} to {}", current, referenced);
                batch.add(referenced);
                queued.add(referenced);
            }
        }

        for (UUID id : bulk) {
            log.info("Copying bulk segment {} from primary", id);
            copySegmentFromPrimary(client, id);
        }

        for (UUID id : data) {
            log.info("Copying data segment {} from primary", id);
            copySegmentFromPrimary(client, id);
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

