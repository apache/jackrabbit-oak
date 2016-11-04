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

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;


public interface DocumentNodeStateCache {
    DocumentNodeStateCache NOOP = new DocumentNodeStateCache() {
        @Override
        public AbstractDocumentNodeState getDocumentNodeState(String path, @Nullable RevisionVector rootRevision,
                                                        RevisionVector lastRev) {
            return null;
        }

        @Override
        public boolean isCached(String path) {
            return false;
        }
    };

    /**
     * Get the node for the given path and revision.
     *
     * @param path the path of the node.
     * @param rootRevision revision of root NodeState
     * @param lastRev last revision of the node at given path
     *
     * @return nodeState at given path or null. If given revision is not present or the
     * path is not cached then <code>null</code> would be returned
     */
    @CheckForNull
    AbstractDocumentNodeState getDocumentNodeState(String path, RevisionVector rootRevision, RevisionVector lastRev);

    /**
     * Determines if given path is cached by this implementation
     * @param path path to check
     * @return true if given path is cached
     */
    boolean isCached(String path);


}
