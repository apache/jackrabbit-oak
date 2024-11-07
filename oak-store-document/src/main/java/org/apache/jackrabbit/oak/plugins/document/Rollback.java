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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * Performs a rollback of a commit based on a list of changes. Changes are
 * rolled back by applying the reverse operation to the document store.
 */
class Rollback {

    static final Rollback FAILED = new Rollback(Revision.newRevision(0),
            Collections.emptyList(), "", 1) {

        @Override
        void perform(@NotNull DocumentStore store) throws DocumentStoreException {
            throw new DocumentStoreException("rollback failed");
        }
    };

    static final Rollback NONE = new Rollback(Revision.newRevision(0),
            Collections.emptyList(), "", 1) {

        @Override
        void perform(@NotNull DocumentStore store) throws DocumentStoreException {
        }
    };

    private final Revision revision;

    private final List<UpdateOp> changed;

    private final String commitRootId;

    private final int batchSize;

    /**
     * Creates a new rollback for the given commit revision.
     *
     * @param revision the commit revision.
     * @param changed the changes to revert.
     * @param commitRootId the id of the commit root document.
     * @param batchSize the batch size for the rollback operations.
     */
    Rollback(@NotNull Revision revision,
             @NotNull List<UpdateOp> changed,
             @NotNull String commitRootId,
             int batchSize) {
        this.revision = revision;
        this.changed = requireNonNull(changed);
        this.commitRootId = requireNonNull(commitRootId);
        this.batchSize = batchSize;
    }

    /**
     * Performs the rollback. If this operation fails with a
     * {@link DocumentStoreException}, then only some of the rollback may have
     * been performed.
     *
     * @param store the store where to apply the rollback.
     * @throws DocumentStoreException if any of the operations fails.
     */
    void perform(@NotNull DocumentStore store) throws DocumentStoreException {
        requireNonNull(store);
        List<UpdateOp> reverseOps = new ArrayList<>();
        for (UpdateOp op : changed) {
            UpdateOp reverse = op.getReverseOperation();
            if (op.isNew()) {
                NodeDocument.setDeletedOnce(reverse);
            }
            // do not create document if it doesn't exist
            reverse.setNew(false);
            reverseOps.add(reverse);
        }
        for (List<UpdateOp> ops : CollectionUtils.partitionList(reverseOps, batchSize)) {
            store.createOrUpdate(NODES, ops);
        }
        removeCollisionMarker(store, commitRootId);
    }

    private void removeCollisionMarker(DocumentStore store, String id) {
        UpdateOp removeCollision = new UpdateOp(id, false);
        NodeDocument.removeCollision(removeCollision, revision);
        store.findAndUpdate(NODES, removeCollision);
    }
}
