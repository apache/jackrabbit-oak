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

package org.apache.jackrabbit.oak.segment.tool;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.tool.Utils.parseSegmentInfoTimestamp;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class SearchNodes {

    public static Builder builder() {
        return new Builder();
    }

    private interface Matcher {

        boolean matches(NodeState node);

    }

    public enum Output {
        TEXT, JOURNAL
    }

    public static class Builder {

        private File path;

        private List<Matcher> matchers = new ArrayList<>();

        private Output output = Output.TEXT;

        private PrintStream out = System.out;

        private PrintStream err = System.err;

        public Builder withPath(File path) {
            this.path = path;
            return this;
        }

        public Builder withProperty(String name) {
            requireNonNull(name, "name");
            matchers.add(node -> node.hasProperty(name));
            return this;
        }

        public Builder withChild(String name) {
            requireNonNull(name, "name");
            matchers.add(node -> node.hasChildNode(name));
            return this;
        }

        public Builder withValue(String name, String value) {
            requireNonNull(name, "name");
            requireNonNull(value, "value");
            matchers.add(node -> {
                PropertyState p = node.getProperty(name);
                if (p == null) {
                    return false;
                }
                if (p.isArray()) {
                    for (String v : p.getValue(Type.STRINGS)) {
                        if (v.equals(value)) {
                            return true;
                        }
                    }
                    return false;
                }
                return p.getValue(Type.STRING).equals(value);
            });
            return this;
        }

        public Builder withOutput(Output output) {
            this.output = requireNonNull(output, "output");
            return this;
        }

        public Builder withOut(PrintStream out) {
            this.out = requireNonNull(out, "out");
            return this;
        }

        public Builder withErr(PrintStream err) {
            this.err = requireNonNull(err, "err");
            return this;
        }

        public SearchNodes build() {
            checkArgument(path != null, "path not specified");
            return new SearchNodes(this);
        }

    }

    private final File path;

    private final List<Matcher> matchers;

    private final Output output;

    private final PrintStream out;

    private final PrintStream err;

    private final Set<String> notFoundSegments = new HashSet<>();

    private SearchNodes(Builder builder) {
        this.path = builder.path;
        this.matchers = new ArrayList<>(builder.matchers);
        this.output = builder.output;
        this.out = builder.out;
        this.err = builder.err;
    }

    public int run() {
        try (ReadOnlyFileStore fileStore = newFileStore()) {
            for (SegmentId segmentId : fileStore.getSegmentIds()) {
                try {
                    processSegment(fileStore, segmentId);
                } catch (SegmentNotFoundException e) {
                    handle(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace(err);
            return 1;
        }
        return 0;
    }

    private ReadOnlyFileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(path).buildReadOnly();
    }

    private void processSegment(ReadOnlyFileStore fileStore, SegmentId segmentId) {
        if (segmentId.isBulkSegmentId()) {
            return;
        }

        Long timestamp = parseSegmentInfoTimestamp(segmentId);

        if (timestamp == null) {
            err.printf("No timestamp found in segment %s\n", segmentId);
            return;
        }

        segmentId.getSegment().forEachRecord((number, type, offset) -> {
            if (type != RecordType.NODE) {
                return;
            }
            try {
                processRecord(fileStore, timestamp, new RecordId(segmentId, number));
            } catch (SegmentNotFoundException e) {
                handle(e);
            }
        });
    }

    private void processRecord(ReadOnlyFileStore fileStore, long timestamp, RecordId recordId) {
        SegmentNodeState nodeState = fileStore.getReader().readNode(recordId);

        boolean matches = true;

        for (Matcher matcher : matchers) {
            matches = matches && matcher.matches(nodeState);
        }

        if (!matches) {
            return;
        }

        switch (output) {
            case TEXT:
                out.printf("%d\t%s\n", timestamp, recordId);
                break;
            case JOURNAL:
                out.printf("%s root %d\n", recordId.toString10(), timestamp);
                break;
            default:
                throw new IllegalStateException("unrecognized output");
        }

    }

    private void handle(SegmentNotFoundException e) {
        if (notFoundSegments.add(e.getSegmentId())) {
            e.printStackTrace(err);
        }
    }

}
