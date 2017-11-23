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

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isPropertyName;

/**
 * A <code>Collision</code> happens when a commit modifies a node, which was
 * also modified in another branch not visible to the current session. This
 * includes the following situations:
 * <ul>
 * <li>Our commit goes to trunk and another session committed to a branch
 * not yet merged back.</li>
 * <li>Our commit goes to a branch and another session committed to trunk
 * or some other branch.</li>
 * </ul>
 * Other collisions like concurrent commits to trunk are handled earlier and
 * do not require collision marking.
 * See {@link Commit#createOrUpdateNode(DocumentStore, UpdateOp)}.
 */
class Collision {

    private static final Logger LOG = LoggerFactory.getLogger(Collision.class);

    private final NodeDocument document;
    private final Revision theirRev;
    private final UpdateOp ourOp;
    private final Revision ourRev;
    private final RevisionContext context;

    Collision(@Nonnull NodeDocument document,
              @Nonnull Revision theirRev,
              @Nonnull UpdateOp ourOp,
              @Nonnull Revision ourRev,
              @Nonnull RevisionContext context) {
        this.document = checkNotNull(document);
        this.theirRev = checkNotNull(theirRev);
        this.ourOp = checkNotNull(ourOp);
        this.ourRev = checkNotNull(ourRev);
        this.context = checkNotNull(context);
    }

    /**
     * Marks the collision in the document store. Either our or their
     * revision is annotated with a collision marker. Their revision is
     * marked if it is not yet committed, otherwise our revision is marked.
     *
     * @param store the document store.
     * @return the revision that was marked. Either our or their.
     * @throws DocumentStoreException if the mark operation fails.
     * @throws IllegalStateException if neither their nor our revision can be
     *              marked because both are already committed.
     */
    @Nonnull
    Revision mark(DocumentStore store) throws DocumentStoreException {
        // first try to mark their revision
        if (markCommitRoot(document, theirRev, ourRev, store, context)) {
            return theirRev;
        }
        // their commit wins, we have to mark ourRev
        NodeDocument newDoc = Collection.NODES.newDocument(store);
        document.deepCopy(newDoc);
        UpdateUtils.applyChanges(newDoc, ourOp);
        if (!markCommitRoot(newDoc, ourRev, theirRev, store, context)) {
            throw new IllegalStateException("Unable to annotate our revision "
                    + "with collision marker. Our revision: " + ourRev
                    + ", their revision: " + theirRev + ", document:\n"
                    + newDoc.format());
        }
        return ourRev;
    }

    /**
     * Returns {@code true} if this is a conflicting collision, {@code false}
     * otherwise.
     *
     * @return {@code true} if this is a conflicting collision, {@code false}
     *              otherwise.
     * @throws DocumentStoreException
     */
    boolean isConflicting() throws DocumentStoreException {
        // did their revision create or delete the node?
        if (document.getDeleted().containsKey(theirRev)) {
            return true;
        }

        for (Map.Entry<Key, Operation> entry : ourOp.getChanges().entrySet()) {
            String name = entry.getKey().getName();
            if (NodeDocument.isDeletedEntry(name)) {
                // always conflicts because existence changed
                return true;
            }
            if (isPropertyName(name)) {
                if (document.getValueMap(name).containsKey(theirRev)) {
                    // concurrent change on the property
                    return true;
                }
            }
        }
        return false;
    }

    //--------------------------< internal >------------------------------------

    /**
     * Marks the commit root of the change to the given <code>document</code> in
     * <code>revision</code>.
     *
     * @param document the document.
     * @param revision the revision of the commit to annotated with a collision
     *            marker.
     * @param other the revision which detected the collision.
     * @param store the document store.
     * @param context the revision context.
     * @return <code>true</code> if the commit for the given revision was marked
     *         successfully; <code>false</code> otherwise.
     */
    private static boolean markCommitRoot(@Nonnull NodeDocument document,
                                          @Nonnull Revision revision,
                                          @Nonnull Revision other,
                                          @Nonnull DocumentStore store,
                                          @Nonnull RevisionContext context) {
        String p = document.getPath();
        String commitRootPath;
        // first check if we can mark the commit with the given revision
        if (document.containsRevision(revision)) {
            if (isCommitted(context.getCommitValue(revision, document))) {
                // already committed
                return false;
            }
            // node is also commit root, but not yet committed
            // i.e. a branch commit, which is not yet merged
            commitRootPath = p;
        } else {
            // next look at commit root
            commitRootPath = document.getCommitRootPath(revision);
            if (commitRootPath == null) {
                throwNoCommitRootException(revision, document);
            }
        }
        // at this point we have a commitRootPath
        UpdateOp op = new UpdateOp(Utils.getIdFromPath(commitRootPath), false);
        NodeDocument commitRoot = store.find(Collection.NODES, op.getId());
        // check commit status of revision
        if (isCommitted(context.getCommitValue(revision, commitRoot))) {
            return false;
        }
        // check if there is already a collision marker
        if (commitRoot.getLocalMap(NodeDocument.COLLISIONS).containsKey(revision)) {
            // already marked
            return true;
        }
        NodeDocument.addCollision(op, revision, other);
        String commitValue = commitRoot.getLocalRevisions().get(revision);
        if (commitValue == null) {
            // no revision entry yet
            // apply collision marker only if entry is still not there
            op.containsMapEntry(NodeDocument.REVISIONS, revision, false);
        } else {
            // not yet merged branch commit
            // apply collision marker only if branch commit is still not merged
            op.equals(NodeDocument.REVISIONS, revision, commitValue);
        }
        commitRoot = store.findAndUpdate(Collection.NODES, op);
        if (commitRoot == null) {
            // commit state changed meanwhile
            // -> assume revision is now committed
            return false;
        } else {
            // check again if revision is still not committed
            // See OAK-3882
            if (isCommitted(context.getCommitValue(revision, commitRoot))) {
                // meanwhile the change was committed and
                // already moved to a previous document
                // -> remove collision marker again
                UpdateOp revert = new UpdateOp(op.getId(), false);
                NodeDocument.removeCollision(revert, revision);
                store.findAndUpdate(Collection.NODES, op);
                return false;
            }
        }
        // otherwise collision marker was set successfully
        LOG.debug("Marked collision on: {} for {} ({})",
                commitRootPath, p, revision);
        return true;
    }

    private static void throwNoCommitRootException(@Nonnull Revision revision,
                                                   @Nonnull Document document)
                                                           throws DocumentStoreException {
        throw new DocumentStoreException("No commit root for revision: "
                + revision + ", document: " + document.format());
    }
}
