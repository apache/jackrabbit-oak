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
import static org.apache.jackrabbit.oak.segment.RecordId.fromString;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Print debugging information about segments, node records and node record
 * ranges.
 */
public class DebugSegments implements Runnable {

    private static final Pattern SEGMENT_REGEX = Pattern.compile("([0-9a-f-]+)|(([0-9a-f-]+:[0-9a-f]+)(-([0-9a-f-]+:[0-9a-f]+))?)?(/.*)?");

    /**
     * Create a builder for the {@link DebugSegments} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link DebugSegments} command.
     */
    public static class Builder {

        private File path;

        private final List<String> segments = new ArrayList<>();

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
         * Add a segment, node record or node record range. It is mandatory to
         * add at least one of a segment, node record or node record range.
         * <p>
         * A segment is specified by its ID, which is specified as a sequence of
         * hexadecimal digits and dashes. In example, {@code
         * 333dc24d-438f-4cca-8b21-3ebf67c05856}.
         * <p>
         * A node record is specified by its identifier, with an optional path.
         * In example, {@code 333dc24d-438f-4cca-8b21-3ebf67c05856:12345/path/to/child}.
         * If a path is not specified, it is take to be {@code /}. The command
         * will print information about the node provided by record ID and about
         * every child identified by the path.
         * <p>
         * A node range record is specified by two node identifiers separated by
         * a dash. In example, {@code 333dc24d-438f-4cca-8b21-3ebf67c05856:12345-46116fda-7a72-4dbc-af88-a09322a7753a:67890}.
         * The command will perform a diff between the two records and print the
         * result in the JSOP format.
         *
         * @param segment The specification for a segment, a node record or a
         *                node record range.
         * @return this builder.
         */
        public Builder withSegment(String segment) {
            this.segments.add(checkNotNull(segment));
            return this;
        }

        /**
         * Create an executable version of the {@link DebugSegments} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkArgument(!segments.isEmpty());
            return new DebugSegments(this);
        }

    }

    private final File path;

    private final List<String> segments;

    private DebugSegments(Builder builder) {
        this.path = builder.path;
        this.segments = new ArrayList<>(builder.segments);
    }

    @Override
    public void run() {
        try (ReadOnlyFileStore store = openReadOnlyFileStore(path)) {
            debugSegments(store);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void debugSegments(ReadOnlyFileStore store) {
        for (String segment : segments) {
            debugSegment(store, segment);
        }
    }

    private static void debugSegment(ReadOnlyFileStore store, String segment) {
        Matcher matcher = SEGMENT_REGEX.matcher(segment);

        if (!matcher.matches()) {
            System.err.println("Unknown argument: " + segment);
            return;
        }

        if (matcher.group(1) != null) {
            UUID uuid = UUID.fromString(matcher.group(1));
            SegmentId id = store.getSegmentIdProvider().newSegmentId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            System.out.println(id.getSegment());
            return;
        }

        RecordId id1 = store.getRevisions().getHead();
        RecordId id2 = null;

        if (matcher.group(2) != null) {
            id1 = fromString(store.getSegmentIdProvider(), matcher.group(3));
            if (matcher.group(4) != null) {
                id2 = fromString(store.getSegmentIdProvider(), matcher.group(5));
            }
        }

        String path = "/";

        if (matcher.group(6) != null) {
            path = matcher.group(6);
        }

        if (id2 == null) {
            NodeState node = store.getReader().readNode(id1);
            System.out.println("/ (" + id1 + ") -> " + node);
            for (String name : PathUtils.elements(path)) {
                node = node.getChildNode(name);
                RecordId nid = null;
                if (node instanceof SegmentNodeState) {
                    nid = ((SegmentNodeState) node).getRecordId();
                }
                System.out.println("  " + name + " (" + nid + ") -> " + node);
            }
            return;
        }

        NodeState node1 = store.getReader().readNode(id1);
        NodeState node2 = store.getReader().readNode(id2);
        for (String name : PathUtils.elements(path)) {
            node1 = node1.getChildNode(name);
            node2 = node2.getChildNode(name);
        }
        System.out.println(JsopBuilder.prettyPrint(JsopDiff.diffToJsop(node1, node2)));
    }

}
