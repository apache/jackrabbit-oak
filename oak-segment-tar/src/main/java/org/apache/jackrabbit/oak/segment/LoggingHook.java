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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.util.SafeEncode;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

public class LoggingHook implements CommitHook, NodeStateDiff {

    private final Consumer<String> writer;

    private LoggingHook(final Consumer<String> writer) {
        this.writer = writer;
    }

    static LoggingHook newLoggingHook(final Consumer<String> writer) {
        return new LoggingHook(writer);
    }

    public void enter(NodeState before, NodeState after) {
        // do nothing
    }

    public void leave(NodeState before, NodeState after) {
        writer.accept("n!");
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        writer.accept("p+ " + toString(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        writer.accept("p^ " + toString(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        writer.accept("p- " + toString(before));
        return true;
    }

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        } catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        writer.accept("n+ " + safeEncode(name));
        this.enter(null, after);
        boolean ret = after.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, this);
        this.leave(null, after);
        return ret;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        writer.accept("n^ " + safeEncode(name));
        this.enter(before, after);
        boolean ret = after.compareAgainstBaseState(before, this);
        this.leave(before, after);
        return ret;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        writer.accept("n- " + safeEncode(name));
        return true;
    }

    private static String toString(final PropertyState ps) {
        final StringBuilder val = new StringBuilder(); // TODO: an output stream would certainly be better
        val.append(safeEncode(ps.getName()));
        val.append(" <");
        val.append(ps.getType());
        val.append("> ");
        if (ps.getType() == BINARY) {
            val.append("= ");
            final Blob blob = ps.getValue(BINARY);
            appendBlob(val, blob);
        } else if (ps.getType() == BINARIES) {
            val.append("= [");
            ps.getValue(BINARIES).forEach((Blob b) -> {
                appendBlob(val, b);
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        } else if (ps.isArray()) {
            val.append("= [");
            ps.getValue(STRINGS).forEach((String s) -> {
                val.append(safeEncode(s));
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        } else {
            val.append("= ").append(safeEncode(ps.getValue(STRING)));
        }
        return val.toString();
    }

    private static void replaceOrAppendLastChar(StringBuilder b, char oldChar, char newChar) {
        if (b.charAt(b.length() - 1) == oldChar) {
            b.setCharAt(b.length() - 1, newChar);
        } else {
            b.append(newChar);
        }
    }

    private static void appendBlob(StringBuilder sb, Blob blob) {
        final InputStream is = blob.getNewStream();
        final char[] hex = "0123456789ABCDEF".toCharArray();
        int b;
        try {
            while ((b = is.read()) >= 0) {
                sb.append(hex[b >> 4]);
                sb.append(hex[b & 0x0f]);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @NotNull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) {
        this.enter(before, after);
        after.compareAgainstBaseState(before, this);
        this.leave(before, after);
        return after;
    }

}
