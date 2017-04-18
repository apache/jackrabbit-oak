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

import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.cache.Cache;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.collect.ImmutableList.of;

/**
 * Resolves the commit value for a given change revision on a document.
 */
final class CommitValueResolver {

    private static final List<String> COMMIT_ROOT_OR_REVISIONS
            = of(NodeDocument.COMMIT_ROOT, NodeDocument.REVISIONS);

    private final Cache<Revision, String> commitValueCache;

    private final Supplier<RevisionVector> sweepRevisions;

    CommitValueResolver(int cacheSize, Supplier<RevisionVector> sweepRevisions) {
        this.commitValueCache = newBuilder().maximumSize(cacheSize).build();
        this.sweepRevisions = sweepRevisions;
    }

    String resolve(@Nonnull Revision changeRevision,
                   @Nonnull NodeDocument doc) {
        // check cache first
        String value = commitValueCache.getIfPresent(changeRevision);
        if (value != null) {
            return value;
        }
        // need to determine the commit value
        doc = resolveDocument(doc, changeRevision);
        if (doc == null) {
            // the document including its history does not contain a change
            // for the given revision
            return null;
        }
        // at this point 'doc' is guaranteed to have a local entry
        // for the given change revision
        if (sweepRevisions.get().isRevisionNewer(changeRevision)) {
            // change revision is newer than sweep revision
            // resolve the commit value without any short cuts
            value = doc.resolveCommitValue(changeRevision);
        } else {
            // change revision is equal or older than sweep revision
            // there are different cases:
            // - doc is a main document and we have a guarantee that a
            //   potential branch commit is marked accordingly
            // - doc is a split document and the revision is guaranteed
            //   to be committed. the remaining question is whether the
            //   revision is from a branch commit
            NodeDocument.SplitDocType sdt = doc.getSplitDocType();
            if (sdt == NodeDocument.SplitDocType.NONE) {
                // sweeper ensures that all changes on main document
                // before the sweep revision are properly marked with
                // branch commit entry if applicable
                if (doc.getLocalBranchCommits().contains(changeRevision)) {
                    // resolve the commit value the classic way
                    value = doc.resolveCommitValue(changeRevision);
                } else {
                    value = "c";
                }
            } else if (sdt == NodeDocument.SplitDocType.DEFAULT_NO_BRANCH) {
                // split document without branch commits, we don't have
                // to check the commit root, the commit value is always 'c'
                value = "c";
            } else {
                // some other split document type introduced
                // before Oak 1.8 and we don't know if this is a branch
                // commit. first try to resolve
                value = doc.resolveCommitValue(changeRevision);
                if (value == null) {
                    // then it must be a non-branch commit and the
                    // split document with the commit value was
                    // already garbage collected
                    value = "c";
                }
            }
        }
        if (Utils.isCommitted(value)) {
            // only cache committed states
            // e.g. branch commits may be merged later and
            // the commit value will change
            commitValueCache.put(changeRevision, value);
        }
        return value;
    }

    /**
     * Resolves to the document that contains the change with the given
     * revision. If the given document contains a local change for the given
     * revision, then the passed document is returned. Otherwise this method
     * looks up previous documents and returns one with a change for the given
     * revision. This method returns {@code null} if neither the passed document
     * nor any of its previous documents contains a change for the given
     * revision.
     *
     * @param doc the document to resolve for the given change revision.
     * @param changeRevision the revision of a change.
     * @return the document with the change or {@code null} if there is no
     *      document with such a change.
     */
    @CheckForNull
    private NodeDocument resolveDocument(@Nonnull NodeDocument doc,
                                         @Nonnull Revision changeRevision) {
        // check if the document contains the change or we need to
        // look up previous documents for the actual change
        if (doc.getLocalCommitRoot().containsKey(changeRevision)
                || doc.getLocalRevisions().containsKey(changeRevision)) {
            return doc;
        }
        // find the previous document with this change
        // first check if there is a commit root entry for this revision
        NodeDocument d = null;
        for (String p : COMMIT_ROOT_OR_REVISIONS) {
            for (NodeDocument prev : doc.getPreviousDocs(p, changeRevision)) {
                d = prev;
                break;
            }
            if (d != null) {
                break;
            }
        }
        return d;
    }

}
