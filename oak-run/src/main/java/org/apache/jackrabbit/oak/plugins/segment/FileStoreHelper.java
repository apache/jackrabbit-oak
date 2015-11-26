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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.singleton;
import static java.util.Collections.sort;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;

public final class FileStoreHelper {

    public static final String newline = "\n";

    private FileStoreHelper() {
    }

    public static List<String> getTarFiles(FileStore store) {
        List<String> files = newArrayList();
        for (String p : store.getTarReaderIndex().keySet()) {
            files.add(new File(p).getName());
        }
        sort(files, reverseOrder());
        return files;
    }

    public static void getGcRoots(FileStore store, UUID uuidIn,
            Map<UUID, Set<Entry<UUID, String>>> links) throws IOException {
        Deque<UUID> todos = new ArrayDeque<UUID>();
        todos.add(uuidIn);
        while (!todos.isEmpty()) {
            UUID uuid = todos.remove();
            for (String f : getTarFiles(store)) {
                Map<UUID, List<UUID>> graph = store.getTarGraph(f);
                for (Entry<UUID, List<UUID>> g : graph.entrySet()) {
                    if (g.getValue() != null && g.getValue().contains(uuid)) {
                        UUID uuidP = g.getKey();
                        if (!todos.contains(uuidP)) {
                            todos.add(uuidP);
                            Set<Entry<UUID, String>> deps = links.get(uuid);
                            if (deps == null) {
                                deps = newHashSet();
                                links.put(uuid, deps);
                            }
                            deps.add(new SimpleImmutableEntry<UUID, String>(
                                    uuidP, f));
                        }
                    }
                }
            }
        }
    }

    public static void printGcRoots(StringBuilder sb,
            Map<UUID, Set<Entry<UUID, String>>> links, UUID uuid, String space,
            String inc) {
        Set<Entry<UUID, String>> roots = links.remove(uuid);
        if (roots == null || roots.isEmpty()) {
            return;
        }
        // TODO is sorting by file name needed?
        for (Entry<UUID, String> r : roots) {
            sb.append(space + r.getKey() + "[" + r.getValue() + "]");
            sb.append(newline);
            printGcRoots(sb, links, r.getKey(), space + inc, inc);
        }
    }

    /**
     * Write the segment graph of a file store to a stream.
     * <p>
     * The graph is written in
     * <a href="https://gephi.github.io/users/supported-graph-formats/gdf-format/">the Guess GDF format</a>,
     * which is easily imported into <a href="https://gephi.github.io/">Gephi</a>.
     * As GDF only supports integers but the segment time stamps are encoded as long
     * the {@code epoch} argument is used as a negative offset translating all timestamps
     * into a valid int range.
     *
     * @param fileStore     file store to graph
     * @param out           stream to write the graph to
     * @param epoch         epoch (in milliseconds)
     * @throws Exception
     */
    public static void writeSegmentGraph(ReadOnlyStore fileStore, OutputStream out, Date epoch) throws Exception {
        PrintWriter writer = new PrintWriter(out);
        try {
            SegmentNodeState root = fileStore.getHead();

            // Segment graph starting from the segment containing root
            Map<UUID, Set<UUID>> segmentGraph = fileStore.getSegmentGraph(new HashSet<UUID>(singleton(root.getRecordId().asUUID())));

            // All segment in the segment graph
            Set<UUID> segments = newHashSet();
            segments.addAll(segmentGraph.keySet());
            for (Set<UUID> tos : segmentGraph.values()) {
                segments.addAll(tos);
            }

            // Graph of segments containing the head state
            final Map<UUID, Set<UUID>> headGraph = newHashMap();
            final Set<UUID> headSegments = newHashSet();
            new SegmentParser() {
                private void addEdge(RecordId from, RecordId to) {
                    UUID fromUUID = from.asUUID();
                    UUID toUUID = to.asUUID();
                    if (!fromUUID.equals(toUUID)) {
                        Set<UUID> tos = headGraph.get(fromUUID);
                        if (tos == null) {
                            tos = newHashSet();
                            headGraph.put(fromUUID, tos);
                        }
                        tos.add(toUUID);
                        headSegments.add(fromUUID);
                        headSegments.add(toUUID);
                    }
                }
                @Override
                protected void onNode(RecordId parentId, RecordId nodeId) {
                    super.onNode(parentId, nodeId);
                    addEdge(parentId, nodeId);
                }
                @Override
                protected void onTemplate(RecordId parentId, RecordId templateId) {
                    super.onTemplate(parentId, templateId);
                    addEdge(parentId, templateId);
                }
                @Override
                protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
                    super.onMap(parentId, mapId, map);
                    addEdge(parentId, mapId);
                }
                @Override
                protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
                    super.onMapDiff(parentId, mapId, map);
                    addEdge(parentId, mapId);
                }
                @Override
                protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
                    super.onMapLeaf(parentId, mapId, map);
                    addEdge(parentId, mapId);
                }
                @Override
                protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
                    super.onMapBranch(parentId, mapId, map);
                    addEdge(parentId, mapId);
                }
                @Override
                protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                    super.onProperty(parentId, propertyId, template);
                    addEdge(parentId, propertyId);
                }
                @Override
                protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
                    super.onValue(parentId, valueId, type);
                    addEdge(parentId, valueId);
                }
                @Override
                protected void onBlob(RecordId parentId, RecordId blobId) {
                    super.onBlob(parentId, blobId);
                    addEdge(parentId, blobId);
                }
                @Override
                protected void onString(RecordId parentId, RecordId stringId) {
                    super.onString(parentId, stringId);
                    addEdge(parentId, stringId);
                }
                @Override
                protected void onList(RecordId parentId, RecordId listId, int count) {
                    super.onList(parentId, listId, count);
                    addEdge(parentId, listId);
                }
                @Override
                protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
                    super.onListBucket(parentId, listId, index, count, capacity);
                    addEdge(parentId, listId);
                }
            }.parseNode(root.getRecordId());

            writer.write("nodedef>name VARCHAR, label VARCHAR, type VARCHAR, wid VARCHAR, gc INT, t INT, head BOOLEAN\n");
            for (UUID segment : segments) {
                writeNode(segment, writer, headSegments.contains(segment), epoch, fileStore.getTracker());
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, head BOOLEAN\n");
            for (Entry<UUID, Set<UUID>> edge : segmentGraph.entrySet()) {
                UUID from = edge.getKey();
                for (UUID to : edge.getValue()) {
                    Set<UUID> he = headGraph.get(from);
                    boolean inHead = he != null && he.contains(to);
                    writer.write(from + "," + to + "," + inHead + "\n");
                }
            }
        } finally {
            writer.close();
        }
    }

    private static void writeNode(UUID node, PrintWriter writer, boolean inHead, Date epoch, SegmentTracker tracker) {
        Map<String, String> sInfo = getSegmentInfo(node, tracker);
        if (sInfo == null) {
            writer.write(node + ",b,bulk,b,-1,-1," + inHead + "\n");
        } else {
            long t = asLong(sInfo.get("t"));
            long ts = t - epoch.getTime();
            checkArgument(ts >= Integer.MIN_VALUE && ts <= Integer.MAX_VALUE,
                    "Time stamp (" + new Date(t) + ") not in epoch (" +
                    new Date(epoch.getTime() + Integer.MIN_VALUE) + " - " +
                    new Date(epoch.getTime() + Integer.MAX_VALUE) + ")");
            writer.write(node +
                    "," + sInfo.get("sno") +
                    ",data" +
                    "," + sInfo.get("wid") +
                    "," + sInfo.get("gc") +
                    "," + ts +
                    "," + inHead + "\n");
        }
    }

    private static long asLong(String string) {
        return Long.valueOf(string);
    }

    private static Map<String, String> getSegmentInfo(UUID node, SegmentTracker tracker) {
        if (isDataSegmentId(node.getLeastSignificantBits())) {
            SegmentId id = tracker.getSegmentId(node.getMostSignificantBits(), node.getLeastSignificantBits());
            String info = id.getSegment().getSegmentInfo();
            if (info != null) {
                JsopTokenizer tokenizer = new JsopTokenizer(info);
                tokenizer.read('{');
                return JsonObject.create(tokenizer).getProperties();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static List<String> readRevisions(File store) {
        File journal = new File(store, "journal.log");
        if (!journal.exists()) {
            return newArrayList();
        }

        List<String> revs = newArrayList();
        JournalReader journalReader = null;
        try {
            journalReader = new JournalReader(journal);
            try {
                revs = newArrayList(journalReader.iterator());
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
            }
        }
        return revs;
    }

}
