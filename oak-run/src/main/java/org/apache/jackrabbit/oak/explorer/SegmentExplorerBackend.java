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

package org.apache.jackrabbit.oak.explorer;

import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper;
import org.apache.jackrabbit.oak.plugins.segment.PCMAnalyser;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStateHelper;
import org.apache.jackrabbit.oak.plugins.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentExplorerBackend implements ExplorerBackend {

    private final File path;

    private FileStore.ReadOnlyStore store;

    private Map<String, Set<UUID>> index;

    SegmentExplorerBackend(File path) throws IOException {
        this.path = path;
    }

    @Override
    public void open() throws IOException {
        try {
            store = FileStore.builder(path).buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }

        index = store.getTarReaderIndex();
    }

    @Override
    public void close() {
        store.close();
        store = null;
        index = null;
    }

    @Override
    public List<String> readRevisions() {
        return FileStoreHelper.readRevisions(path);
    }

    @Override
    public Map<String, Set<UUID>> getTarReaderIndex() {
        return store.getTarReaderIndex();
    }

    @Override
    public Map<UUID, List<UUID>> getTarGraph(String file) throws IOException {
        return store.getTarGraph(file);
    }

    @Override
    public List<String> getTarFiles() {
        return FileStoreHelper.getTarFiles(store);
    }

    @Override
    public void getGcRoots(UUID uuidIn, Map<UUID, Set<Entry<UUID, String>>> links) throws IOException {
        FileStoreHelper.getGcRoots(store, uuidIn, links);
    }

    @Override
    public Set<UUID> getReferencedSegmentIds() {
        Set<UUID> ids = newHashSet();

        for (SegmentId id : store.getTracker().getReferencedSegmentIds()) {
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
        return new SegmentNodeState(RecordId.fromString(store.getTracker(), recordId));
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
        Map<UUID, String> result = Maps.newHashMap();

        for (SegmentId segmentId : SegmentBlob.getBulkSegmentIds(blob)) {
            result.put(segmentId.asUUID(), getFile(segmentId));
        }

        return result;
    }

    @Override
    public String getPersistedCompactionMapStats() {
        return new PCMAnalyser(store).toString();
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
        for (Entry<String, Set<UUID>> path2Uuid : index.entrySet()) {
            for (UUID uuid : path2Uuid.getValue()) {
                if (uuid.equals(segmentId.asUUID())) {
                    return new File(path2Uuid.getKey()).getName();
                }
            }
        }
        return null;
    }

}
