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

package org.apache.jackrabbit.oak.plugins.segment.file.tooling;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.json.JsonSerializer.DEFAULT_FILTER_EXPRESSION;

import java.io.File;
import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility for tracing a node back through the revision history.
 */
@Deprecated
public class RevisionHistory {
    private final ReadOnlyStore store;

    /**
     * Create a new instance for a {@link FileStore} in the given {@code directory}.
     *
     * @param directory
     * @throws IOException
     */
    @Deprecated
    public RevisionHistory(@Nonnull File directory) throws IOException, InvalidFileStoreVersionException {
        this.store = FileStore.builder(checkNotNull(directory)).buildReadOnly();
    }

    private static NodeState getNode(SegmentNodeState root, String path) {
        NodeState node = root;
        for (String name : elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /**
     * Return the history of the node at the given {@code path} according to the passed
     * {@code journal}.
     *
     * @param journal
     * @param path
     * @return
     * @throws IOException
     */
    @Deprecated
    public Iterable<HistoryElement> getHistory(@Nonnull File journal, @Nonnull final String path)
            throws IOException {
        checkNotNull(path);
        return transform(new JournalReader(checkNotNull(journal)),
            new Function<String, HistoryElement>() {
                @Nullable @Override
                public HistoryElement apply(String revision) {
                    store.setRevision(revision);
                    NodeState node = getNode(store.getHead(), path);
                    return new HistoryElement(revision, node);
                }
        });
    }

    /**
     * Representation of a point in time for a given node.
     */
    @Deprecated
    public static final class HistoryElement {
        private final String revision;
        private final NodeState node;

        HistoryElement(String revision, NodeState node) {
            this.revision = revision;
            this.node = node;
        }

        /**
         * Revision of the node
         * @return
         */
        @Nonnull
        @Deprecated
        public String getRevision() {
            return revision;
        }

        /**
         * Node at given revision
         * @return
         */
        @CheckForNull
        @Deprecated
        public NodeState getNode() {
            return node;
        }

        /**
         * Serialise this element to JSON up to the given {@code depth}.
         * @param depth
         * @return
         */
        @Deprecated
        public String toString(int depth) {
            JsonSerializer json = new JsonSerializer(depth, 0, Integer.MAX_VALUE,
                DEFAULT_FILTER_EXPRESSION, new BlobSerializer());
            json.serialize(node);
            return revision + "=" + json;
        }

        /**
         * @return  {@code toString(0)}
         */
        @Override
        @Deprecated
        public String toString() {
            return toString(0);
        }

        @Override
        @Deprecated
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            HistoryElement that = (HistoryElement) other;
            return revision.equals(that.revision) &&
                (node == null ? that.node == null : node.equals(that.node));

        }

        @Override
        @Deprecated
        public int hashCode() {
            return 31 * revision.hashCode() +
                (node != null ? node.hashCode() : 0);
        }
    }
}
