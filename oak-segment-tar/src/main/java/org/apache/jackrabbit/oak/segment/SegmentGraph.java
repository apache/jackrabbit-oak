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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonMap;
import static java.util.regex.Pattern.compile;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;

/**
 * Utility graph for parsing a segment graph.
 */
public final class SegmentGraph {
    private SegmentGraph() { }

    /**
     * Visitor for receiving call backs while traversing the
     * segment graph.
     */
    public interface SegmentGraphVisitor {

        /**
         * A call to this method indicates that the {@code from} segment
         * references the {@code to} segment. Or if {@code to} is {@code null}
         * that the {@code from} has no references.
         *
         * @param from
         * @param to
         */
        void accept(@Nonnull UUID from, @CheckForNull UUID to);
    }

    /**
     * A simple graph representation for a graph with node of type {@code T}.
     */
    public static class Graph<T> {
        /** The vertices of this graph */
        private final Set<T> vertices = newHashSet();

        /** The edges of this graph */
        private final Map<T, Multiset<T>> edges = newHashMap();

        private void addVertex(T vertex) {
            vertices.add(vertex);
        }

        private void addEdge(T from, T to) {
            Multiset<T> tos = edges.get(from);
            if (tos == null) {
                tos = HashMultiset.create();
                edges.put(from, tos);
            }
            tos.add(to);
        }

        /**
         * @return  the vertices of this graph
         */
        public Iterable<T> vertices() {
            return vertices;
        }

        /**
         * @param vertex
         * @return  {@code true} iff this graph contains {@code vertex}
         */
        public boolean containsVertex(T vertex) {
            return vertices.contains(vertex);
        }

        /**
         * @return  the edges of this graph
         */
        public Set<Entry<T, Multiset<T>>> edges() {
            return edges.entrySet();
        }

        /**
         * @param from
         * @return  the edges from {@code from} or {@code null} if none.
         */
        public Multiset<T> getEdge(T from) {
            return edges.get(from);
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
     * @param pattern       regular expression specifying inclusion of nodes or {@code null}
     *                      for all nodes.
     * @throws Exception
     */
    public static void writeSegmentGraph(
            @Nonnull ReadOnlyStore fileStore,
            @Nonnull OutputStream out,
            @Nonnull Date epoch,
            @CheckForNull String pattern) throws Exception {
        checkNotNull(epoch);
        PrintWriter writer = new PrintWriter(checkNotNull(out));
        try {
            SegmentNodeState root = checkNotNull(fileStore).getHead();
            Predicate<UUID> filter = pattern == null
                ? Predicates.<UUID>alwaysTrue()
                : createRegExpFilter(pattern, fileStore);
            Graph<UUID> segmentGraph = parseSegmentGraph(fileStore, filter);
            Graph<UUID> headGraph = parseHeadGraph(fileStore.getReader(), root.getRecordId());

            writer.write("nodedef>name VARCHAR, label VARCHAR, type VARCHAR, wid VARCHAR, gc INT, t INT, size INT, head BOOLEAN\n");
            for (UUID segment : segmentGraph.vertices()) {
                writeNode(segment, writer, headGraph.containsVertex(segment), epoch, fileStore);
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, head BOOLEAN\n");
            for (Entry<UUID, Multiset<UUID>> edge : segmentGraph.edges()) {
                UUID from = edge.getKey();
                for (UUID to : edge.getValue()) {
                    if (!from.equals(to)) {
                        Multiset<UUID> he = headGraph.getEdge(from);
                        boolean inHead = he != null && he.contains(to);
                        writer.write(from + "," + to + "," + inHead + "\n");
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    /**
     * Create a regular expression based inclusion filter for segment.
     *
     * @param pattern       regular expression specifying inclusion of nodes.
     * @param store       the segment store acting upon.
     * @return
     */
    public static Predicate<UUID> createRegExpFilter(
            @Nonnull String pattern,
            @Nonnull final SegmentStore store) {
        final Pattern regExp = compile(checkNotNull(pattern));
        checkNotNull(store);

        return new Predicate<UUID>() {
            @Override
            public boolean apply(UUID segment) {
                try {
                    String info = getSegmentInfo(segment, store);
                    if (info == null) {
                        info = "NULL";
                    }
                    return regExp.matcher(info).matches();
                } catch (Exception e) {
                    System.err.println("Error accessing segment " + segment + ": " + e);
                    return false;
                }
            }
        };
    }

    /**
     * Parse the segment graph of a file store.
     *
     * @param fileStore     file store to parse
     * @param filter        inclusion criteria for vertices and edges. An edge is only included if
     *                      both its source and target vertex are included.
     * @return the segment graph rooted as the segment containing the head node
     *         state of {@code fileStore}.
     * @throws IOException
     */
    @Nonnull
    public static Graph<UUID> parseSegmentGraph(
            @Nonnull ReadOnlyStore fileStore,
            @Nonnull Predicate<UUID> filter) throws IOException {
        SegmentNodeState root = checkNotNull(fileStore).getHead();
        HashSet<UUID> roots = newHashSet(root.getRecordId().asUUID());
        return parseSegmentGraph(fileStore, roots, filter, Functions.<UUID>identity());
    }

    /**
     * Write the gc generation graph of a file store to a stream.
     * <p>
     * The graph is written in
     * <a href="https://gephi.github.io/users/supported-graph-formats/gdf-format/">the Guess GDF format</a>,
     * which is easily imported into <a href="https://gephi.github.io/">Gephi</a>.
     *
     * @param fileStore     file store to graph
     * @param out           stream to write the graph to
     * @throws Exception
     */
    public static void writeGCGraph(@Nonnull ReadOnlyStore fileStore, @Nonnull OutputStream out)
            throws Exception {
        PrintWriter writer = new PrintWriter(checkNotNull(out));
        try {
            Graph<String> gcGraph = parseGCGraph(checkNotNull(fileStore));

            writer.write("nodedef>name VARCHAR\n");
            for (String gen : gcGraph.vertices()) {
                writer.write(gen + "\n");
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, weight INT\n");
            for (Entry<String, Multiset<String>> edge : gcGraph.edges()) {
                String from = edge.getKey();
                Multiset<String> tos = edge.getValue();
                for (String to : tos.elementSet()) {
                    if (!from.equals(to) && !to.isEmpty()) {
                        writer.write(from + "," + to + "," + tos.count(to) + "\n");
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    /**
     * Parse the gc generation graph of a file store.
     *
     * @param fileStore     file store to parse
     * @return the gc generation graph rooted ad the segment containing the head node
     *         state of {@code fileStore}.
     * @throws IOException
     */
    @Nonnull
    public static Graph<String> parseGCGraph(@Nonnull final ReadOnlyStore fileStore)
            throws IOException {
        SegmentNodeState root = checkNotNull(fileStore).getHead();
        HashSet<UUID> roots = newHashSet(root.getRecordId().asUUID());
        return parseSegmentGraph(fileStore, roots, Predicates.<UUID>alwaysTrue(), new Function<UUID, String>() {
            @Override @Nullable
            public String apply(UUID segmentId) {
                Map<String, String> info = new SegmentInfo(segmentId, fileStore).getInfoMap();
                String error = info.get("error");
                if (error != null) {
                    return "Error";
                } else {
                    return info.get("gc");
                }
            }
        });
    }

    /**
     * Parse the segment graph of a file store starting with a given set of root segments.
     * The full segment graph is mapped through the passed {@code map} to the
     * graph returned by this function.
     *
     * @param fileStore     file store to parse
     * @param roots         the initial set of segments
     * @param map           map defining an homomorphism from the segment graph into the returned graph
     * @param filter        inclusion criteria for vertices and edges. An edge is only included if
     *                      both its source and target vertex are included.
     * @return   the segment graph of {@code fileStore} rooted at {@code roots} and mapped
     *           by {@code map}
     * @throws IOException
     */
    @Nonnull
    public static <T> Graph<T> parseSegmentGraph(
            @Nonnull final ReadOnlyStore fileStore,
            @Nonnull Set<UUID> roots,
            @Nonnull final Predicate<UUID> filter,
            @Nonnull final Function<UUID, T> map) throws IOException {
        final Graph<T> graph = new Graph<T>();

        checkNotNull(filter);
        checkNotNull(map);
        checkNotNull(fileStore).traverseSegmentGraph(checkNotNull(roots),
            new SegmentGraphVisitor() {
                @Override
                public void accept(@Nonnull UUID from, @CheckForNull UUID to) {
                    T fromT = null;
                    T toT = null;
                    if (filter.apply(from)) {
                        fromT = map.apply(from);
                        graph.addVertex(fromT);
                    }
                    if (to != null && filter.apply(to)) {
                        toT = map.apply(to);
                        graph.addVertex(toT);
                    }
                    if (fromT != null && toT != null) {
                        graph.addEdge(fromT, toT);
                    }
                }
            });
        return graph;
    }

    /**
     * Parser the head graph of segment store. The head graph is the sub graph of the segment
     * graph containing the {@code root}.
     * @param reader  segment reader for the store to parse
     * @param root
     * @return  the head graph of {@code root}.
     */
    @Nonnull
    public static Graph<UUID> parseHeadGraph(
            @Nonnull SegmentReader reader,
            @Nonnull RecordId root) {
        final Graph<UUID> graph = new Graph<UUID>();

        try {
            new SegmentParser(reader) {
                private void addEdge(RecordId from, RecordId to) {
                    graph.addVertex(from.asUUID());
                    graph.addVertex(to.asUUID());
                    graph.addEdge(from.asUUID(), to.asUUID());
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
            }.parseNode(checkNotNull(root));
        } catch (SegmentNotFoundException e) {
            System.err.println("Error head graph parsing: " + e);
        }
        return graph;
    }

    private static void writeNode(UUID node, PrintWriter writer, boolean inHead, Date epoch, SegmentStore store) {
        SegmentInfo segmentInfo = new SegmentInfo(node, store);
        if (!segmentInfo.isData()) {
            writer.write(node + ",b,bulk,b,-1,-1," + inHead + "\n");
        } else {
            Map<String, String> sInfo = segmentInfo.getInfoMap();
            String error = sInfo.get("error");
            if (error != null) {
                writer.write(node +
                    "," + firstLine(error) +
                    ",error,e,-1,-1," + inHead + "\n");
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
                    "," + sInfo.get("size") +
                    "," + inHead + "\n");
            }
        }
    }

    private static String firstLine(String string) {
        BufferedReader reader = new BufferedReader(new StringReader(string));
        try {
            return reader.readLine();
        } catch (IOException e) {
            return string;
        } finally {
            closeQuietly(reader);
        }
    }

    private static long asLong(String string) {
        return Long.valueOf(string);
    }

    private static String getSegmentInfo(UUID segment, SegmentStore store) {
        return new SegmentInfo(segment, store).getInfo();
    }

    private static class SegmentInfo {

        private final UUID uuid;

        private final SegmentStore store;

        private SegmentId id;

        SegmentInfo(UUID uuid, SegmentStore store) {
            this.uuid = uuid;
            this.store = store;
        }

        boolean isData() {
            return isDataSegmentId(uuid.getLeastSignificantBits());
        }

        SegmentId getSegmentId() {
            if (id == null) {
                long msb = uuid.getMostSignificantBits();
                long lsb = uuid.getLeastSignificantBits();
                id = store.newSegmentId(msb, lsb);
            }
            return id;
        }

        String getInfo() {
            if (isData()) {
                return getSegmentId().getSegment().getSegmentInfo();
            } else {
                return null;
            }
        }

        Map<String, String> getInfoMap() {
            try {
                Map<String, String> infoMap = newHashMap();
                String info = getInfo();
                if (info != null) {
                    JsopTokenizer tokenizer = new JsopTokenizer(info);
                    tokenizer.read('{');
                    infoMap.putAll(JsonObject.create(tokenizer).getProperties());
                }
                Segment segment = getSegmentId().getSegment();
                infoMap.put("size", valueOf(segment.size()));
                infoMap.put("gc", valueOf(segment.getGcGeneration()));
                return infoMap;
            } catch (SegmentNotFoundException e) {
                return singletonMap("error", getStackTraceAsString(e));
            }
        }

    }

}
