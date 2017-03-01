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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.RecordUsageAnalyser;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;

/**
 * Print debugging information about a segment store.
 */
public class DebugStore implements Runnable {

    /**
     * Create a builder for the {@link DebugStore} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link DebugStore} command.
     */
    public static class Builder {

        private File path;

        private Builder() {
            // Prevent external instantiation
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * Create an executable version of the {@link DebugStore} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            return new DebugStore(this);
        }

    }

    private final File path;

    private DebugStore(Builder builder) {
        this.path = builder.path;
    }

    @Override
    public void run() {
        try (ReadOnlyFileStore store = openReadOnlyFileStore(path)) {
            debugFileStore(store);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<SegmentId> getReferencedSegmentIds(ReadOnlyFileStore store, Segment segment) {
        List<SegmentId> result = new ArrayList<>();

        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            UUID uuid = segment.getReferencedSegmentId(i);
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();
            result.add(store.getSegmentIdProvider().newSegmentId(msb, lsb));
        }

        return result;
    }

    private static void debugFileStore(ReadOnlyFileStore store) {
        Map<SegmentId, List<SegmentId>> idmap = Maps.newHashMap();
        int dataCount = 0;
        long dataSize = 0;
        int bulkCount = 0;
        long bulkSize = 0;

        RecordUsageAnalyser analyser = new RecordUsageAnalyser(store.getReader());

        for (SegmentId id : store.getSegmentIds()) {
            if (id.isDataSegmentId()) {
                Segment segment = id.getSegment();
                dataCount++;
                dataSize += segment.size();
                idmap.put(id, getReferencedSegmentIds(store, segment));
                analyseSegment(segment, analyser);
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
                idmap.put(id, Collections.<SegmentId>emptyList());
            }
        }
        System.out.println("Total size:");
        System.out.format("%s in %6d data segments%n", byteCountToDisplaySize(dataSize), dataCount);
        System.out.format("%s in %6d bulk segments%n", byteCountToDisplaySize(bulkSize), bulkCount);
        System.out.println(analyser.toString());

        Set<SegmentId> garbage = newHashSet(idmap.keySet());
        Queue<SegmentId> queue = Queues.newArrayDeque();
        queue.add(store.getRevisions().getHead().getSegmentId());
        while (!queue.isEmpty()) {
            SegmentId id = queue.remove();
            if (garbage.remove(id)) {
                queue.addAll(idmap.get(id));
            }
        }
        dataCount = 0;
        dataSize = 0;
        bulkCount = 0;
        bulkSize = 0;
        for (SegmentId id : garbage) {
            if (id.isDataSegmentId()) {
                dataCount++;
                dataSize += id.getSegment().size();
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
            }
        }
        System.out.format("%nAvailable for garbage collection:%n");
        System.out.format("%s in %6d data segments%n", byteCountToDisplaySize(dataSize), dataCount);
        System.out.format("%s in %6d bulk segments%n", byteCountToDisplaySize(bulkSize), bulkCount);
    }

    private static void analyseSegment(final Segment segment, final RecordUsageAnalyser analyser) {
        final List<RecordId> ids = newArrayList();

        segment.forEachRecord(new RecordConsumer() {

            @Override
            public void consume(int number, RecordType type, int offset) {
                if (type == NODE) {
                    ids.add(new RecordId(segment.getSegmentId(), number));
                }
            }

        });

        for (RecordId id : ids) {
            try {
                analyser.analyseNode(id);
            } catch (Exception e) {
                System.err.format("Error while processing node at %s", id);
                e.printStackTrace();
            }
        }
    }

}
