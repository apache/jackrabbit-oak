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

import org.apache.jackrabbit.mongomk.util.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A <code>Collision</code> happens when a commit modifies a node, which was
 * also modified in a branch commit, but the branch commit is not yet merged.
 */
class Collision {

    private static final Logger LOG = LoggerFactory.getLogger(Collision.class);

    private final Map<String, Object> document;
    private final String theirRev;
    private final UpdateOp ourOp;
    private final String ourRev;

    Collision(@Nonnull Map<String, Object> document,
              @Nonnull Revision theirRev,
              @Nonnull UpdateOp ourOp,
              @Nonnull Revision ourRev) {
        this.document = checkNotNull(document);
        this.theirRev = checkNotNull(theirRev).toString();
        this.ourOp = checkNotNull(ourOp);
        this.ourRev = checkNotNull(ourRev).toString();
    }

    boolean mark(DocumentStore store) {
        if (markCommitRoot(document, theirRev, store)) {
            return true;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) document.get(UpdateOp.REVISIONS);
        if (revisions.containsKey(theirRev)) {
            String value = revisions.get(theirRev);
            if ("true".equals(value)) {
                // their commit wins, we have to mark ourRev
                Map<String, Object> newDoc = Utils.newMap();
                Utils.deepCopyMap(document, newDoc);
                MemoryDocumentStore.applyChanges(newDoc, ourOp);
                if (markCommitRoot(newDoc, ourRev, store)) {
                    return true;
                }
            }
        }
        return true;
    }

    private static boolean markCommitRoot(@Nonnull Map<String, Object> document,
                                          @Nonnull String revision,
                                          @Nonnull DocumentStore store) {
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
                // TODO: detect concurrent commit of previously un-merged changes
                // TODO: check _commitRoot for revision is not 'true'
                store.createOrUpdate(DocumentStore.Collection.NODES, op);
                LOG.debug("Marked collision on: {} for {} ({})",
                        new Object[]{commitRootPath, p, revision});
                return true;
            }
        }
        return false;
    }
}
