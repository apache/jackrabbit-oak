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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newTreeSet;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStateHelper.getTemplateId;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.jcr.PropertyType;

import com.google.common.escape.Escapers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Print information about one or more TAR files from an existing segment store.
 */
public class DebugTars implements Runnable {

    /**
     * Create a builder for the {@link DebugTars} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link DebugTars} command.
     */
    public static class Builder {

        private File path;

        private final List<String> tars = new ArrayList<>();

        private final int maxCharDisplay = Integer.getInteger("max.char.display", 60);

        private Builder() {
            // Prevent external instantiation.
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
         * Add a TAR file. The command will print information about every TAR
         * file added via this method. It is mandatory to add at least one TAR
         * file.
         *
         * @param tar the name of a TAR file.
         * @return this builder.
         */
        public Builder withTar(String tar) {
            checkArgument(tar.endsWith(".tar"));
            this.tars.add(tar);
            return this;
        }

        /**
         * Create an executable version of the {@link DebugTars} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkArgument(!tars.isEmpty());
            return new DebugTars(this);
        }

    }

    private final File path;

    private final List<String> tars;

    private final int maxCharDisplay;

    private DebugTars(Builder builder) {
        this.path = builder.path;
        this.tars = new ArrayList<>(builder.tars);
        this.maxCharDisplay = builder.maxCharDisplay;
    }

    @Override
    public void run() {
        try (ReadOnlyFileStore store = openReadOnlyFileStore(path)) {
            debugTarFiles(store);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void debugTarFiles(ReadOnlyFileStore store) {
        for (String tar : tars) {
            debugTarFile(store, tar);
        }
    }

    private void debugTarFile(ReadOnlyFileStore store, String t) {
        File tar = new File(path, t);

        if (!tar.exists()) {
            System.out.println("file doesn't exist, skipping " + t);
            return;
        }

        System.out.println("Debug file " + tar + "(" + tar.length() + ")");
        Set<UUID> uuids = new HashSet<UUID>();
        boolean hasRefs = false;

        for (Map.Entry<String, Set<UUID>> e : store.getTarReaderIndex().entrySet()) {
            if (e.getKey().endsWith(t)) {
                hasRefs = true;
                uuids = e.getValue();
            }
        }

        if (hasRefs) {
            System.out.println("SegmentNodeState references to " + t);
            List<String> paths = new ArrayList<String>();
            filterNodeStates(uuids, paths, store.getHead(), "/");
            for (String p : paths) {
                System.out.println("  " + p);
            }
        } else {
            System.out.println("No references to " + t);
        }

        try {
            Map<UUID, Set<UUID>> graph = store.getTarGraph(t);
            System.out.println();
            System.out.println("Tar graph:");
            for (Map.Entry<UUID, Set<UUID>> entry : graph.entrySet()) {
                System.out.println("" + entry.getKey() + '=' + entry.getValue());
            }
        } catch (IOException e) {
            System.out.println("Error getting tar graph:");
            e.printStackTrace();
        }
    }

    private void filterNodeStates(Set<UUID> uuids, List<String> paths, SegmentNodeState state, String path) {
        Set<String> localPaths = newTreeSet();
        for (PropertyState ps : state.getProperties()) {
            if (ps instanceof SegmentPropertyState) {
                SegmentPropertyState sps = (SegmentPropertyState) ps;
                RecordId recordId = sps.getRecordId();
                UUID id = recordId.getSegmentId().asUUID();
                if (uuids.contains(id)) {
                    if (ps.getType().tag() == PropertyType.STRING) {
                        String val = "";
                        if (ps.count() > 0) {
                            // only shows the first value, do we need more?
                            val = displayString(ps.getValue(Type.STRING, 0));
                        }
                        localPaths.add(getLocalPath(path, ps, val, recordId));
                    } else {
                        localPaths.add(getLocalPath(path, ps, recordId));
                    }

                }
                if (ps.getType().tag() == PropertyType.BINARY) {
                    // look for extra segment references
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i);
                        for (SegmentId sbid : SegmentBlob.getBulkSegmentIds(b)) {
                            UUID bid = sbid.asUUID();
                            if (!bid.equals(id) && uuids.contains(bid)) {
                                localPaths.add(getLocalPath(path, ps, recordId));
                            }
                        }
                    }
                }
            }
        }

        RecordId stateId = state.getRecordId();
        if (uuids.contains(stateId.getSegmentId().asUUID())) {
            localPaths.add(path + " [SegmentNodeState@" + stateId + "]");
        }

        RecordId templateId = getTemplateId(state);
        if (uuids.contains(templateId.getSegmentId().asUUID())) {
            localPaths.add(path + "[Template@" + templateId + "]");
        }
        paths.addAll(localPaths);
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            NodeState c = ce.getNodeState();
            if (c instanceof SegmentNodeState) {
                filterNodeStates(uuids, paths, (SegmentNodeState) c,
                        path + ce.getName() + "/");
            }
        }
    }

    private static String getLocalPath(String path, PropertyState ps, String value, RecordId id) {
        return path + ps.getName() + " = " + value + " [SegmentPropertyState<" + ps.getType() + ">@" + id + "]";
    }

    private static String getLocalPath(String path, PropertyState ps, RecordId id) {
        return path + ps + " [SegmentPropertyState<" + ps.getType() + ">@" + id + "]";
    }

    private String displayString(String value) {
        if (maxCharDisplay > 0 && value.length() > maxCharDisplay) {
            value = value.substring(0, maxCharDisplay) + "... (" + value.length() + " chars)";
        }

        String escaped = Escapers.builder()
                .setSafeRange(' ', '~')
                .addEscape('"', "\\\"")
                .addEscape('\\', "\\\\")
                .build()
                .escape(value);

        return '"' + escaped + '"';
    }

}
