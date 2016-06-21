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

package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;

public interface DocumentNodeStateCache {
    DocumentNodeStateCache NOOP = new DocumentNodeStateCache() {
        @Nonnull
        @Override
        public NodeStateCacheEntry getDocumentNodeState(String path, @Nullable RevisionVector rootRevision,
                                                        RevisionVector parentLastRev) {
            return UNKNOWN;
        }
    };

    NodeStateCacheEntry MISSING = new NodeStateCacheEntry(NodeStateCacheEntry.EntryType.MISSING);

    NodeStateCacheEntry UNKNOWN = new NodeStateCacheEntry(NodeStateCacheEntry.EntryType.UNKNOWN);

    /**
     * Get the node for the given path and revision.
     *
     * @param path the path of the node.
     * @param rootRevision
     * @param readRevision the read revision.
     * @return the node or {@link MISSING} if no state is there for given path and revision or {@link UNKNOWN} if
     * cache does not have any knowledge of nodeState for given parameters
     */
    @Nonnull
    NodeStateCacheEntry getDocumentNodeState(String path, RevisionVector rootRevision,
                                             RevisionVector parentLastRev);

    class NodeStateCacheEntry {
        private enum EntryType {FOUND, MISSING, UNKNOWN}
        private final AbstractDocumentNodeState state;
        private final EntryType entryType;

        public NodeStateCacheEntry(AbstractDocumentNodeState state) {
            this.state = state;
            this.entryType = EntryType.FOUND;
        }

        private NodeStateCacheEntry(EntryType entryType) {
            this.state = null;
            this.entryType = entryType;
        }

        public AbstractDocumentNodeState getState(){
            checkState(entryType == EntryType.FOUND, "Cannot read state from an entry of type [%s]", entryType);
            return state;
        }

        public boolean isUnknown(){
            return entryType == EntryType.UNKNOWN;
        }

        public boolean isMissing(){
            return entryType == EntryType.MISSING;
        }

        public boolean isFound(){
            return entryType == EntryType.FOUND;
        }
    }
}
