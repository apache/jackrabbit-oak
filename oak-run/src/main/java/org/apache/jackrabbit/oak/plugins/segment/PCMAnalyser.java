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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Queues.newArrayDeque;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.segment.PersistedCompactionMap.PERSISTED_COMPACTION_MAP;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;

import java.io.File;
import java.util.Deque;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;

/**
 * RecordUsageAnalyser tailored to extract PersistedCompactionMap history and
 * size
 */
public class PCMAnalyser extends RecordUsageAnalyser {

    /**
     * Extracts persisted compaction map information. Returns a list of
     * compaction map chains
     */
    private static List<Deque<PCMInfo>> readPCMHistory(FileStore store) {
        List<Deque<PCMInfo>> pcms = newArrayList();
        Deque<PCMInfo> chain = newArrayDeque();

        Map<String, Set<UUID>> index = store.getTarReaderIndex();
        for (String path : index.keySet()) {
            Set<UUID> segments = index.get(path);
            String name = new File(path).getName();

            for (UUID id : segments) {
                if (!isDataSegmentId(id.getLeastSignificantBits())) {
                    continue;
                }
                Segment s = readSegment(store, id);
                for (int r = 0; r < s.getRootCount(); r++) {
                    if (s.getRootType(r) == RecordType.VALUE) {
                        RecordId nodeId = new RecordId(s.getSegmentId(),
                                s.getRootOffset(r));
                        String v = Segment.readString(nodeId);
                        PCMInfo pcm = parsePCMInfo(v, store, name);
                        if (pcm != null) {
                            if (!pcm.sameMap(chain.peekLast())) {
                                pcms.add(chain);
                                chain = newArrayDeque();
                            }
                            chain.addLast(pcm);
                        }
                    }
                }
            }
        }
        if (!chain.isEmpty()) {
            pcms.add(chain);
        }
        return pcms;
    }

    /**
     * Extracts persisted compaction map information, if available, otherwise
     * returs null
     */
    private static PCMInfo parsePCMInfo(String mapInfo, FileStore store,
            String file) {
        if (mapInfo == null || !mapInfo.startsWith(PERSISTED_COMPACTION_MAP)) {
            return null;
        }
        SegmentTracker tracker = store.getTracker();
        int idStartIndex = mapInfo.indexOf("id=") + 3;
        int idEndIndex = mapInfo.indexOf(",", idStartIndex);
        String id = mapInfo.substring(idStartIndex, idEndIndex);
        RecordId rid = null;
        try {
            rid = RecordId.fromString(tracker, id);
        } catch (IllegalArgumentException iae) {
            // log a warn?
            return null;
        }

        int baseStartIndex = mapInfo.indexOf("baseId=") + 7;
        String base = mapInfo.substring(baseStartIndex, mapInfo.length() - 1);
        RecordId bid = null;
        if (!"null".equals(base)) {
            try {
                bid = RecordId.fromString(tracker, base);
            } catch (IllegalArgumentException iae) {
                // log a warn?
            }
        }
        return new PCMInfo(rid, bid, file);
    }

    private static Segment readSegment(FileStore store, UUID id) {
        return store.readSegment(new SegmentId(store.getTracker(), id
                .getMostSignificantBits(), id.getLeastSignificantBits()));
    }

    private final List<Deque<PCMInfo>> pcms;
    private final Set<String> errors = newHashSet();

    public PCMAnalyser(FileStore store) {
        pcms = readPCMHistory(store);
        for (Deque<PCMInfo> pcm : pcms) {
            try {
                onPCM(pcm.getFirst().getId());
            } catch (IllegalStateException ex) {
                ex.printStackTrace();
                errors.add(ex.getMessage());
            }
        }
    }

    private void onPCM(RecordId recordId) {
        Segment s = recordId.getSegment();
        MapRecord map = s.readMap(recordId);
        parseMap(null, recordId, map);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        @SuppressWarnings("resource")
        Formatter formatter = new Formatter(sb);
        if (pcms.isEmpty()) {
            formatter.format("No persisted compaction map found.%n");
        } else {
            formatter.format("Persisted compaction map info:%n");
            for (Deque<PCMInfo> pcm : pcms) {
                formatter.format("%s%n", pcm);
            }
            formatter.format("Persisted compaction map size:%n");
            sb.append(super.toString());

            formatter.format("%n");
            for (String e : errors) {
                formatter.format("%s%n", e);
            }
        }
        return sb.toString();
    }

    private static class PCMInfo {

        private final RecordId id;
        private final RecordId baseId;
        private final String file;

        public PCMInfo(RecordId id, RecordId baseId, String file) {
            this.id = checkNotNull(id);
            this.baseId = baseId;
            this.file = file;
        }

        @Override
        public String toString() {
            return id + "[" + file + "]";
        }

        public RecordId getId() {
            return id;
        }

        public RecordId getBaseId() {
            return baseId;
        }

        /**
         * Determines if the current PCM can be considered as the next link in
         * the current compaction map. If provided 'o' is null, then the current
         * PCM is the head
         */
        public boolean sameMap(PCMInfo o) {
            if (o == null) {
                return true;
            }
            return id.equals(o.getBaseId());
        }
    }

}
