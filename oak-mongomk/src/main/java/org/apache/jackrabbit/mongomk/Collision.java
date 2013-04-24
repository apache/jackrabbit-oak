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
package org.apache.jackrabbit.mongomk;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A <code>Collision</code> happens when a commit modifies a node, which was
 * also modified in a branch commit, but the branch commit is not yet merged.
 */
class Collision {

    private final Map<String, Object> document;
    private final String revision;

    Collision(@Nonnull Map<String, Object> document,
              @Nonnull Revision revision) {
        this.document = checkNotNull(document);
        this.revision = checkNotNull(revision).toString();
    }

    boolean mark(DocumentStore store) {
        @SuppressWarnings("unchecked")
        Map<String, Integer> commitRoots = (Map<String, Integer>) document.get(UpdateOp.COMMIT_ROOT);
        if (commitRoots != null) {
            Integer depth = commitRoots.get(revision);
            if (depth != null) {
                String p = Utils.getPathFromId((String) document.get(UpdateOp.ID));
                String commitRootPath = PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - depth);
                UpdateOp op = new UpdateOp(commitRootPath,
                        Utils.getIdFromPath(commitRootPath), false);
                op.setMapEntry(UpdateOp.COLLISIONS, revision, true);
                store.createOrUpdate(DocumentStore.Collection.NODES, op);
            }
        }
        // TODO: detect concurrent commit of previously un-merged changes
        return true;
    }
}
