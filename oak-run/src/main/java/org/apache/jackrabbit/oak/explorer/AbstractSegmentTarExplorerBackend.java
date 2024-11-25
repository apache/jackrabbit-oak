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
package org.apache.jackrabbit.oak.explorer;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStateHelper;
import org.apache.jackrabbit.oak.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.reverseOrder;

/**
 * Abstraction for Segment-Tar based backends.
 */
public abstract class AbstractSegmentTarExplorerBackend implements ExplorerBackend {
    protected ReadOnlyFileStore store;
    protected Map<String, Set<UUID>> index;


    @Override
    public abstract void open() throws IOException;

    @Override
    public void close() {
        store.close();
        store = null;
        index = null;
    }

    abstract protected JournalFile getJournal();

    @Override
    public List<String> readRevisions() {
        JournalFile journal = getJournal();

        if (!journal.exists()) {
            return new ArrayList<>();
        }

        List<String> revs = new ArrayList<>();
        JournalReader journalReader = null;

        try {
            journalReader = new JournalReader(journal);
            Iterator<String> revisionIterator = Iterators.transform(journalReader,
                    entry -> entry.getRevision());

            try {
                revs = CollectionUtils.toList(revisionIterator);
            } finally {
                journalReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (journalReader != null) {
                    journalReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return revs;
    }

    @Override
    public Map<String, Set<UUID>> getTarReaderIndex() {
        return store.getTarReaderIndex();
    }

    @Override
    public Map<UUID, Set<UUID>> getTarGraph(String file) throws IOException {
        return store.getTarGraph(file);
    }

    @Override
    public List<String> getTarFiles() {
        List<String> files = new ArrayList<>(store.getTarReaderIndex().keySet());
        files.sort(reverseOrder());
        return files;
    }

    @Override
    public void getGcRoots(UUID uuidIn, Map<UUID, Set<Map.Entry<UUID, String>>> links) throws IOException {
        Deque<UUID> todos = new ArrayDeque<UUID>();
        todos.add(uuidIn);
        Set<UUID> visited = new HashSet<>();
        while (!todos.isEmpty()) {
            UUID uuid = todos.remove();
            if (!visited.add(uuid)) {
                continue;
            }
            for (String f : getTarFiles()) {
                Map<UUID, Set<UUID>> graph = store.getTarGraph(f);
                for (Map.Entry<UUID, Set<UUID>> g : graph.entrySet()) {
                    if (g.getValue() != null && g.getValue().contains(uuid)) {
                        UUID uuidP = g.getKey();
                        if (!todos.contains(uuidP)) {
                            todos.add(uuidP);
                            Set<Map.Entry<UUID, String>> deps = links.get(uuid);
                            if (deps == null) {
                                deps = new HashSet<>();
                                links.put(uuid, deps);
                            }
                            deps.add(new AbstractMap.SimpleImmutableEntry<UUID, String>(
                                    uuidP, f));
                        }
                    }
                }
            }
        }
    }

    @Override
    public Set<UUID> getReferencedSegmentIds() {
        Set<UUID> ids = new HashSet<>();

        for (SegmentId id : store.getReferencedSegmentIds()) {
            ids.add(id.asUUID());
        }

        return ids;
    }

    @Override
    public NodeState getHead() {
        return store.getHead();
    }

    @Override
    public NodeState readNodeState(String recordId) {
        return store.getReader().readNode(RecordId.fromString(store.getSegmentIdProvider(), recordId));
    }

    @Override
    public void setRevision(String revision) {
        store.setRevision(revision);
    }

    @Override
    public boolean isPersisted(NodeState state) {
        return state instanceof SegmentNodeState;
    }

    @Override
    public boolean isPersisted(PropertyState state) {
        return state instanceof SegmentPropertyState;
    }

    @Override
    public String getRecordId(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getRecordId((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public UUID getSegmentId(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getSegmentId((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public String getRecordId(PropertyState state) {
        if (state instanceof SegmentPropertyState) {
            return getRecordId((SegmentPropertyState) state);
        }

        return null;
    }

    @Override
    public UUID getSegmentId(PropertyState state) {
        if (state instanceof SegmentPropertyState) {
            return getSegmentId((SegmentPropertyState) state);
        }

        return null;
    }

    @Override
    public String getTemplateRecordId(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getTemplateRecordId((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public UUID getTemplateSegmentId(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getTemplateSegmentId((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public String getFile(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getFile((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public String getFile(PropertyState state) {
        if (state instanceof SegmentPropertyState) {
            return getFile((SegmentPropertyState) state);
        }

        return null;
    }

    @Override
    public String getTemplateFile(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return getTemplateFile((SegmentNodeState) state);
        }

        return null;
    }

    @Override
    public Map<UUID, String> getBulkSegmentIds(Blob blob) {
        Map<UUID, String> result = new HashMap<>();

        for (SegmentId segmentId : SegmentBlob.getBulkSegmentIds(blob)) {
            result.put(segmentId.asUUID(), getFile(segmentId));
        }

        return result;
    }

    @Override
    public String getPersistedCompactionMapStats() {
        return "";
    }

    @Override
    public boolean isExternal(Blob blob) {
        if (blob instanceof SegmentBlob) {
            return isExternal((SegmentBlob) blob);
        }

        return false;
    }

    private boolean isExternal(SegmentBlob blob) {
        return blob.isExternal();
    }

    private String getRecordId(SegmentNodeState state) {
        return state.getRecordId().toString();
    }

    private UUID getSegmentId(SegmentNodeState state) {
        return state.getRecordId().getSegmentId().asUUID();
    }

    private String getRecordId(SegmentPropertyState state) {
        return state.getRecordId().toString();
    }

    private UUID getSegmentId(SegmentPropertyState state) {
        return state.getRecordId().getSegmentId().asUUID();
    }

    private String getTemplateRecordId(SegmentNodeState state) {
        RecordId recordId = SegmentNodeStateHelper.getTemplateId(state);

        if (recordId == null) {
            return null;
        }

        return recordId.toString();
    }

    private UUID getTemplateSegmentId(SegmentNodeState state) {
        RecordId recordId = SegmentNodeStateHelper.getTemplateId(state);

        if (recordId == null) {
            return null;
        }

        return recordId.getSegmentId().asUUID();
    }

    private String getFile(SegmentNodeState state) {
        return getFile(state.getRecordId().getSegmentId());
    }

    private String getFile(SegmentPropertyState state) {
        return getFile(state.getRecordId().getSegmentId());
    }

    private String getTemplateFile(SegmentNodeState state) {
        RecordId recordId = SegmentNodeStateHelper.getTemplateId(state);

        if (recordId == null) {
            return null;
        }

        return getFile(recordId.getSegmentId());
    }

    private String getFile(SegmentId segmentId) {
        for (Map.Entry<String, Set<UUID>> nameToId : index.entrySet()) {
            for (UUID uuid : nameToId.getValue()) {
                if (uuid.equals(segmentId.asUUID())) {
                    return nameToId.getKey();
                }
            }
        }
        return null;
    }
}
