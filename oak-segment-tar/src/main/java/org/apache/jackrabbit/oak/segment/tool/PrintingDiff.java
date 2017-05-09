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

import static com.google.common.collect.Iterables.transform;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.PrintWriter;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

final class PrintingDiff implements NodeStateDiff {

    private static final Function<Blob, String> BLOB_LENGTH = new Function<Blob, String>() {

        @Override
        public String apply(Blob b) {
            return safeGetLength(b);
        }

        private String safeGetLength(Blob b) {
            try {
                return byteCountToDisplaySize(b.length());
            } catch (IllegalStateException e) {
                // missing BlobStore probably
            }
            return "[N/A]";
        }

    };

    private final PrintWriter pw;

    private final String path;

    PrintingDiff(PrintWriter pw, String path) {
        this.pw = pw;
        this.path = path;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        pw.println("    + " + toString(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        pw.println("    ^ " + before.getName());
        pw.println("      - " + toString(before));
        pw.println("      + " + toString(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        pw.println("    - " + toString(before));
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String p = concat(path, name);
        pw.println("+ " + p);
        return after.compareAgainstBaseState(EMPTY_NODE, new PrintingDiff(pw, p));
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        String p = concat(path, name);
        pw.println("^ " + p);
        return after.compareAgainstBaseState(before, new PrintingDiff(pw, p));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String p = concat(path, name);
        pw.println("- " + p);
        return MISSING_NODE.compareAgainstBaseState(before, new PrintingDiff(pw, p));
    }

    private static String toString(PropertyState ps) {
        StringBuilder val = new StringBuilder();
        val.append(ps.getName()).append("<").append(ps.getType()).append(">");
        if (ps.getType() == BINARY) {
            String v = BLOB_LENGTH.apply(ps.getValue(BINARY));
            val.append(" = {").append(v).append("}");
        } else if (ps.getType() == BINARIES) {
            String v = transform(ps.getValue(BINARIES), BLOB_LENGTH).toString();
            val.append("[").append(ps.count()).append("] = ").append(v);
        } else if (ps.isArray()) {
            val.append("[").append(ps.count()).append("] = ").append(ps.getValue(STRINGS));
        } else {
            val.append(" = ").append(ps.getValue(STRING));
        }
        return ps.getName() + "<" + ps.getType() + ">" + val.toString();
    }

}
