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

import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;

public class VersionGCSupport {

    private static final Logger LOG = LoggerFactory.getLogger(VersionGCSupport.class);

    private final DocumentStore store;

    public VersionGCSupport(DocumentStore store) {
        this.store = store;
    }

    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        return filter(getAllDocuments(store), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return input.wasDeletedOnce() && !input.hasBeenModifiedSince(lastModifiedTime);
            }
        });
    }

    public void deleteSplitDocuments(Set<SplitDocType> gcTypes,
                                     long oldestRevTimeStamp,
                                     VersionGCStats stats) {
        stats.splitDocGCCount += createCleanUp(gcTypes, oldestRevTimeStamp, stats)
                // .disconnect() TODO: work in progress
                .deleteSplitDocuments();
    }

    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new SplitDocumentCleanUp(gcTypes, oldestRevTimeStamp, stats);
    }

    protected class SplitDocumentCleanUp {

        protected final Set<SplitDocType> gcTypes;
        protected final long oldestRevTimeStamp;
        protected final VersionGCStats stats;

        protected SplitDocumentCleanUp(Set<SplitDocType> gcTypes,
                                       long oldestRevTimeStamp,
                                       VersionGCStats stats) {
            this.gcTypes = gcTypes;
            this.oldestRevTimeStamp = oldestRevTimeStamp;
            this.stats = stats;
        }

        protected SplitDocumentCleanUp disconnect() {
            for (NodeDocument splitDoc : identifyGarbage()) {
                disconnect(splitDoc);
            }
            return this;
        }

        protected int deleteSplitDocuments() {
            List<String> docsToDelete = Lists.newArrayList(
                    transform(identifyGarbage(), new Function<NodeDocument, String>() {
                        @Override
                        public String apply(NodeDocument input) {
                            return input.getId();
                        }
                    }));
            store.remove(NODES, docsToDelete);
            return docsToDelete.size();
        }

        protected Iterable<NodeDocument> identifyGarbage() {
            return filter(getAllDocuments(store), new Predicate<NodeDocument>() {
                @Override
                public boolean apply(NodeDocument doc) {
                    SplitDocType splitType = doc.getSplitDocType();
                    return gcTypes.contains(splitType)
                            && doc.hasAllRevisionLessThan(oldestRevTimeStamp);
                }
            });
        }

        protected void disconnect(NodeDocument splitDoc) {
            String splitId = splitDoc.getId();
            String mainId = Utils.getIdFromPath(splitDoc.getMainPath());
            NodeDocument doc = store.find(NODES, mainId);
            if (doc == null) {
                LOG.warn("Main document {} already removed. Split document is {}",
                        mainId, splitId);
                return;
            }
            int slashIdx = splitId.lastIndexOf('/');
            int height = Integer.parseInt(splitId.substring(slashIdx + 1));
            Revision rev = Revision.fromString(
                    splitId.substring(splitId.lastIndexOf('/', slashIdx - 1) + 1, slashIdx));
            doc = findReferencingDoc(doc, rev, height);
            if (doc == null) {
                LOG.warn("Split document {} not referenced anymore. Main document is {}",
                        splitId, mainId);
                return;
            }
            // remove reference
            UpdateOp update = new UpdateOp(doc.getId(), false);
            NodeDocument.removePrevious(update, rev);
            NodeDocument old = store.findAndUpdate(NODES, update);
            if (old != null
                    && old.getSplitDocType() == SplitDocType.INTERMEDIATE
                    && old.getPreviousRanges().size() == 1
                    && old.getPreviousRanges().containsKey(rev)) {
                // this was the last reference on an intermediate split doc
                disconnect(old);
                store.remove(NODES, old.getId());
                stats.intermediateSplitDocGCCount++;
            }
        }

        @CheckForNull
        protected NodeDocument findReferencingDoc(NodeDocument current,
                                                  Revision revision,
                                                  int height) {
            for (Range range : current.getPreviousRanges().values()) {
                if (range.getHeight() == height && range.high.equals(revision)) {
                    return current;
                } else if (range.includes(revision)) {
                    String prevId = Utils.getPreviousIdFor(
                            current.getMainPath(), range.high, range.height);
                    NodeDocument prev = store.find(NODES, prevId);
                    if (prev == null) {
                        LOG.warn("Split document {} does not exist anymore. Main document is {}",
                                prevId, Utils.getIdFromPath(current.getMainPath()));
                        continue;
                    }
                    // recurse into the split hierarchy
                    NodeDocument doc = findReferencingDoc(prev, revision, height);
                    if (doc != null) {
                        return doc;
                    }
                }

            }
            return null;
        }
    }
}
