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

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
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

    private final StandbyClient client;

    private final Supplier<Boolean> running;

    private final Set<UUID> visited = newHashSet();

    private final Set<UUID> queued = newHashSet();

    private final Map<UUID, Segment> cache = newHashMap();

    StandbyClientSyncExecution(FileStore store, StandbyClient client, Supplier<Boolean> running) {
        this.store = store;
        this.client = client;
        this.running = running;
    }

    void execute() throws Exception {
        RecordId remoteHead = getHead();

        if (remoteHead.equals(store.getHead().getRecordId())) {
            return;
        }

        long t = System.currentTimeMillis();
        SegmentNodeState before = store.getHead();
        SegmentNodeBuilder builder = before.builder();
        SegmentNodeState current = newSegmentNodeState(remoteHead);
        compareAgainstBaseState(current, before, builder);
        boolean ok = setHead(before, builder.getNodeState());
        log.debug("updated head state successfully: {} in {}ms.", ok, System.currentTimeMillis() - t);
    }

    private RecordId getHead() throws Exception {
        return RecordId.fromString(store, client.getHead());
    }

    private SegmentNodeState newSegmentNodeState(RecordId id) {
        return store.getReader().readNode(id);
    }

    private boolean setHead(@Nonnull SegmentNodeState expected, @Nonnull SegmentNodeState head) {
        return store.getRevisions().setHead(expected.getRecordId(), head.getRecordId());
    }

    private boolean compareAgainstBaseState(SegmentNodeState current, SegmentNodeState before, SegmentNodeBuilder builder) throws Exception {
        while (true) {
            try {
                return current.compareAgainstBaseState(before, new StandbyDiff(builder, store, client, running));
            } catch (SegmentNotFoundException e) {
                log.info("Found missing segment {}", e.getSegmentId());
                copySegmentHierarchyFromPrimary(UUID.fromString(e.getSegmentId()));
            }
        }
    }

    private void copySegmentHierarchyFromPrimary(UUID segmentId) throws Exception {
        LinkedList<UUID> batch = new LinkedList<>();

        batch.offer(segmentId);

        while (batch.size() > 0) {
            UUID current = batch.remove();

            log.info("Loading segment {}", current);
            Segment segment = copySegmentFromPrimary(current);

            log.info("Marking segment {} as loaded", current);
            visited.add(current);

            if (!SegmentId.isDataSegmentId(current.getLeastSignificantBits())) {
                continue;
            }

            log.info("Inspecting segment {} for references", current);
            for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
                UUID referenced = segment.getReferencedSegmentId(i);

                // Short circuit for the "back reference problem". The segment
                // graph might or might not be acyclic. The following check
                // prevents processing segment that were already traversed.

                if (visited.contains(referenced)) {
                    continue;
                }

                // Short circuit for the "diamond problem". Imagine that segment S1
                // references S2 and S3 and both S2 and S3 reference S4. These
                // references form the shape of a diamond. If the segments are
                // processed in the order S1, S2, S3, then S4 is added twice to the
                // 'batch' queue. The following check prevents processing S4 twice
                // or more.

                if (queued.contains(referenced)) {
                    continue;
                }

                log.info("Found reference from {} to {}", current, referenced);

                if (SegmentId.isDataSegmentId(referenced.getLeastSignificantBits())) {
                    batch.add(referenced);
                } else {
                    batch.addFirst(referenced);
                }

                queued.add(referenced);
            }
        }
    }

    private Segment copySegmentFromPrimary(UUID uuid) throws Exception {
        Segment result = cache.get(uuid);

        if (result != null) {
            log.info("Segment {} was found in the local cache", uuid);
            return result;
        }

        byte[] data = client.getSegment(uuid.toString());

        if (data == null) {
            throw new IllegalStateException("Unable to read segment " + uuid);
        }

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        SegmentId segmentId = store.newSegmentId(msb, lsb);
        store.writeSegment(segmentId, data, 0, data.length);
        result = segmentId.getSegment();
        cache.put(uuid, result);
        return result;
    }

}

