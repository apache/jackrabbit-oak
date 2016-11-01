/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static java.lang.Integer.getInteger;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.encode;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code PartialCompactionMap} implementation persisting its entries
 * to segments.
 *
 * TODO In theory we could also compact the compaction map. Is there any need to do so?
 */
@Deprecated
public class PersistedCompactionMap implements PartialCompactionMap {
    private static final Logger LOG = LoggerFactory.getLogger(PersistedCompactionMap.class);

    /**
     * Rough estimate of the number of bytes of disk space of a map entry.
     * Used by the compaction gain estimator to offset its estimate.
     */
    @Deprecated
    public static final int BYTES_PER_ENTRY = getInteger("bytes-per-entry", 50);

    /**
     * Number of map entries to keep until compressing this map.
     */
    private static final int COMPRESS_INTERVAL = getInteger("compress-interval", 10000000);

    /**
     * Key used to store meta data associated with the individual map generations. Tools
     * can use this to grep across segments for finding the meta data and ultimately
     * to find and parse the compaction map generations.
     */
    @Deprecated
    public static final String PERSISTED_COMPACTION_MAP = "PersistedCompactionMap";

    private final TreeMap<UUID, RecordIdMap> recent = newTreeMap();

    private final SegmentTracker tracker;

    private long recordCount;
    private MapRecord entries;

    PersistedCompactionMap(@Nonnull SegmentTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @Deprecated
    public boolean wasCompactedTo(@Nonnull RecordId before, @Nonnull RecordId after) {
        return (after.equals(get(before)));
    }

    @Override
    @Deprecated
    public boolean wasCompacted(@Nonnull UUID uuid) {
        return recent.containsKey(uuid) ||
                entries != null && entries.getEntry(uuid.toString()) != null;
    }

    private static UUID asUUID(@Nonnull SegmentId id) {
        return new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    @Override
    @CheckForNull
    @Deprecated
    public RecordId get(@Nonnull RecordId before) {
        UUID uuid = asUUID(before.getSegmentId());
        short offset = encode(before.getOffset());

        RecordId recordId = get(recent, uuid, offset);
        if (recordId != null) {
            return recordId;
        }

        return get(tracker, entries, uuid, offset);
    }

    @CheckForNull
    private static RecordId get(@Nonnull Map<UUID, RecordIdMap> map, @Nonnull UUID uuid, short offset) {
        RecordIdMap newSegment = map.get(uuid);
        if (newSegment != null) {
            return newSegment.get(offset);
        }
        return null;
    }

    @CheckForNull
    private static RecordId get(@Nonnull SegmentTracker tracker, @Nullable MapRecord map, @Nonnull UUID uuid, short offset) {
        if (map == null) {
            return null;
        }

        MapEntry newSegmentId = map.getEntry(uuid.toString());
        if (newSegmentId == null) {
            return null;
        }
        MapRecord newSegment = new MapRecord(newSegmentId.getValue());
        MapEntry newRecordId = newSegment.getEntry(String.valueOf(offset));
        if (newRecordId == null) {
            return null;
        }
        return RecordId.fromString(tracker, Segment.readString(newRecordId.getValue()));
    }

    @Override
    @Deprecated
    public void put(@Nonnull RecordId before, @Nonnull RecordId after) {
        if (get(before) != null) {
            throw new IllegalArgumentException();
        }

        UUID uuid = asUUID(before.getSegmentId());
        RecordIdMap entry = recent.get(uuid);
        if (entry == null) {
            entry = new RecordIdMap();
            recent.put(uuid, entry);
        }
        entry.put(encode(before.getOffset()), after);

        if (recent.size() > COMPRESS_INTERVAL) {
            compress();
        }
    }

    @Override
    @Deprecated
    public void remove(@Nonnull Set<UUID> uuids) {
        compress(uuids);
    }

    @Override
    @Deprecated
    public void compress() {
        compress(Collections.<UUID>emptySet());
    }

    @Override
    @Deprecated
    public long getSegmentCount() {
        return entries == null ? 0 : entries.size();
    }

    @Override
    @Deprecated
    public long getRecordCount() {
        return recordCount;
    }

    @Override
    @Deprecated
    public boolean isEmpty() {
        return recent.size() + recordCount == 0;
    }

    private void compress(@Nonnull Set<UUID> removed) {
        try {
            if (recent.isEmpty() && removed.isEmpty()) {
                return;
            }

            SegmentWriter writer = null;
            Map<String, RecordId> segmentIdMap = newHashMap();
            for (Entry<UUID, RecordIdMap> recentEntry : recent.entrySet()) {
                UUID uuid = recentEntry.getKey();
                RecordIdMap newSegment = recentEntry.getValue();

                if (removed.contains(uuid)) {
                    continue;
                }

                MapRecord base;
                MapEntry baseEntry = entries == null ? null : entries.getEntry(uuid.toString());
                base = baseEntry == null ? null : new MapRecord(baseEntry.getValue());

                if (writer == null) {
                    writer = tracker.createSegmentWriter(createWid());
                }

                Map<String, RecordId> offsetMap = newHashMap();
                for (int k = 0; k < newSegment.size(); k++) {
                    offsetMap.put(String.valueOf(newSegment.getKey(k)),
                        writer.writeString(newSegment.getRecordId(k).toString10()));
                }
                RecordId newEntryId = writer.writeMap(base, offsetMap).getRecordId();
                segmentIdMap.put(uuid.toString(), newEntryId);
                recordCount += offsetMap.size();
            }

            if (entries != null) {
                for (UUID uuid : removed) {
                    MapEntry toRemove = entries.getEntry(uuid.toString());
                    if (toRemove != null) {
                        segmentIdMap.put(uuid.toString(), null);
                        recordCount -= new MapRecord(toRemove.getValue()).size();
                    }
                }
            }

            if (!segmentIdMap.isEmpty()) {
                if (writer == null) {
                    writer = tracker.createSegmentWriter(createWid());
                }

                RecordId previousBaseId = entries == null ? null : entries.getRecordId();
                entries = writer.writeMap(entries, segmentIdMap);
                entries.getSegment().getSegmentId().pin();
                String mapInfo = PERSISTED_COMPACTION_MAP + '{' +
                    "id=" + entries.getRecordId() +
                    ", baseId=" + previousBaseId + '}';
                writer.writeString(mapInfo);
                writer.flush();
            }

            recent.clear();
            if (recordCount == 0) {
                entries = null;
            }
        } catch (IOException e) {
            LOG.error("Error compression compaction map", e);
            throw new IllegalStateException("Unexpected IOException", e);
        }
    }

    @Nonnull
    private String createWid() {
        return "cm-" + (tracker.getCompactionMap().getGeneration() + 1);
    }

    /**
     * @return 0
     */
    @Override
    @Deprecated
    public long getEstimatedWeight() {
        return 0;
    }


}
