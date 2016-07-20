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

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static com.google.common.collect.Iterators.partition;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;

public class VersionGarbageCollector {
    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private static final int PROGRESS_BATCH_SIZE = 10000;
    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);
    private final DocumentNodeStore nodeStore;
    private final DocumentStore ds;
    private final VersionGCSupport versionStore;
    private int overflowToDiskThreshold = 100000;

    private static final Logger log = LoggerFactory.getLogger(VersionGarbageCollector.class);

    /**
     * Split document types which can be safely garbage collected
     */
    private static final Set<NodeDocument.SplitDocType> GC_TYPES = EnumSet.of(
            DEFAULT_LEAF, COMMIT_ROOT_ONLY);

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport) {
        this.nodeStore = nodeStore;
        this.versionStore = gcSupport;
        this.ds = nodeStore.getDocumentStore();
    }

    public VersionGCStats gc(long maxRevisionAge, TimeUnit unit) throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        Stopwatch sw = Stopwatch.createStarted();
        VersionGCStats stats = new VersionGCStats();
        final long oldestRevTimeStamp = nodeStore.getClock().getTime() - maxRevisionAgeInMillis;
        final RevisionVector headRevision = nodeStore.getHeadRevision();

        log.info("Starting revision garbage collection. Revisions older than [{}] will be " +
                "removed", Utils.timestampToString(oldestRevTimeStamp));

        //Check for any registered checkpoint which prevent the GC from running
        Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
        if (checkpoint != null && checkpoint.getTimestamp() < oldestRevTimeStamp) {
            log.info("Ignoring revision garbage collection because a valid " +
                            "checkpoint [{}] was found, which is older than [{}].",
                    checkpoint.toReadableString(),
                    Utils.timestampToString(oldestRevTimeStamp)
            );
            stats.ignoredGCDueToCheckPoint = true;
            return stats;
        }

        collectDeletedDocuments(stats, headRevision, oldestRevTimeStamp);
        collectSplitDocuments(stats, oldestRevTimeStamp);

        sw.stop();
        log.info("Revision garbage collection finished in {}. {}", sw, stats);
        return stats;
    }

    public void setOverflowToDiskThreshold(int overflowToDiskThreshold) {
        this.overflowToDiskThreshold = overflowToDiskThreshold;
    }

    private void collectSplitDocuments(VersionGCStats stats, long oldestRevTimeStamp) {
        stats.collectAndDeleteSplitDocs.start();
        versionStore.deleteSplitDocuments(GC_TYPES, oldestRevTimeStamp, stats);
        stats.collectAndDeleteSplitDocs.stop();
    }

    private void collectDeletedDocuments(VersionGCStats stats,
                                         RevisionVector headRevision,
                                         long oldestRevTimeStamp)
            throws IOException {
        int docsTraversed = 0;
        DeletedDocsGC gc = new DeletedDocsGC(headRevision);
        try {
            stats.collectDeletedDocs.start();
            Iterable<NodeDocument> itr = versionStore.getPossiblyDeletedDocs(oldestRevTimeStamp);
            try {
                for (NodeDocument doc : itr) {
                    // Check if node is actually deleted at current revision
                    // As node is not modified since oldestRevTimeStamp then
                    // this node has not be revived again in past maxRevisionAge
                    // So deleting it is safe
                    docsTraversed++;
                    if (docsTraversed % PROGRESS_BATCH_SIZE == 0){
                        log.info("Iterated through {} documents so far. {} found to be deleted",
                                docsTraversed, gc.getNumDocuments());
                    }
                    gc.possiblyDeleted(doc);
                }
            } finally {
                Utils.closeIfCloseable(itr);
            }
            stats.collectDeletedDocs.stop();

            if (gc.getNumDocuments() == 0){
                return;
            }

            stats.deleteDeletedDocs.start();

            gc.removeDocuments(stats);

            stats.deleteDeletedDocs.stop();
        } finally {
            gc.close();
        }
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        int deletedDocGCCount;
        int splitDocGCCount;
        int intermediateSplitDocGCCount;
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch collectAndDeleteSplitDocs = Stopwatch.createUnstarted();

        @Override
        public String toString() {
            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    ", deletedDocGCCount=" + deletedDocGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", timeToCollectDeletedDocs=" + collectDeletedDocs +
                    ", timeTakenToDeleteDeletedDocs=" + deleteDeletedDocs +
                    ", timeTakenToCollectAndDeleteSplitDocs=" + collectAndDeleteSplitDocs +
                    '}';
        }
    }

    /**
     * A helper class to remove document for deleted nodes.
     */
    private class DeletedDocsGC implements Closeable {

        private final RevisionVector headRevision;
        private final StringSort docIdsToDelete = newStringSort();
        private final StringSort prevDocIdsToDelete = newStringSort();
        private final Set<String> exclude = Sets.newHashSet();
        private boolean sorted = false;

        public DeletedDocsGC(@Nonnull RevisionVector headRevision) {
            this.headRevision = checkNotNull(headRevision);
        }

        /**
         * @return the number of documents gathers so far that have been
         * identified as garbage via {@link #possiblyDeleted(NodeDocument)}.
         * This number does not include the previous documents.
         */
        long getNumDocuments() {
            return docIdsToDelete.getSize();
        }

        /**
         * Informs the GC that the given document is possibly deleted. The
         * implementation will check if the node still exists at the head
         * revision passed to the constructor to this GC. The implementation
         * will keep track of documents representing deleted nodes and remove
         * them together with associated previous document
         *
         * @param doc the candidate document.
         */
        void possiblyDeleted(NodeDocument doc)
                throws IOException {
            if (doc.getNodeAtRevision(nodeStore, headRevision, null) == null) {
                // construct an id that also contains
                // the _modified time of the document
                String id = doc.getId() + "/" + doc.getModified();
                addDocument(id);
                // Collect id of all previous docs also
                Iterator<NodeDocument> it = doc.getAllPreviousDocs();
                while (it.hasNext()) {
                    addPreviousDocument(it.next().getId());
                }
            }
        }

        /**
         * Removes the documents that have been identified as garbage. This
         * also includes previous documents. This method will only remove
         * documents that have not been modified since they were passed to
         * {@link #possiblyDeleted(NodeDocument)}.
         *
         * @param stats to track the number of removed documents.
         */
        void removeDocuments(VersionGCStats stats) throws IOException {
            stats.deletedDocGCCount += removeDeletedDocuments();
            // FIXME: this is incorrect because that method also removes intermediate docs
            stats.splitDocGCCount += removeDeletedPreviousDocuments();
        }

        public void close() {
            try {
                docIdsToDelete.close();
            } catch (IOException e) {
                log.warn("Failed to close docIdsToDelete", e);
            }
            try {
                prevDocIdsToDelete.close();
            } catch (IOException e) {
                log.warn("Failed to close prevDocIdsToDelete", e);
            }
        }

        //------------------------------< internal >----------------------------

        private void addDocument(String id) throws IOException {
            docIdsToDelete.add(id);
        }

        private long getNumPreviousDocuments() {
            return prevDocIdsToDelete.getSize() - exclude.size();
        }

        private void addPreviousDocument(String id) throws IOException {
            prevDocIdsToDelete.add(id);
        }

        private Iterator<String> getDocIdsToDelete() throws IOException {
            ensureSorted();
            return docIdsToDelete.getIds();
        }

        private void concurrentModification(NodeDocument doc) {
            Iterator<NodeDocument> it = doc.getAllPreviousDocs();
            while (it.hasNext()) {
                exclude.add(it.next().getId());
            }
        }

        private Iterator<String> getPrevDocIdsToDelete() throws IOException {
            ensureSorted();
            return Iterators.filter(prevDocIdsToDelete.getIds(),
                    new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return !exclude.contains(input);
                }
            });
        }

        private int removeDeletedDocuments() throws IOException {
            Iterator<String> docIdsToDelete = getDocIdsToDelete();
            log.info("Proceeding to delete [{}] documents", getNumDocuments());

            Iterator<List<String>> idListItr = partition(docIdsToDelete, DELETE_BATCH_SIZE);
            int deletedCount = 0;
            int lastLoggedCount = 0;
            int recreatedCount = 0;
            while (idListItr.hasNext()) {
                Map<String, Map<Key, Condition>> deletionBatch = Maps.newLinkedHashMap();
                for (String s : idListItr.next()) {
                    int idx = s.lastIndexOf('/');
                    String id = s.substring(0, idx);
                    long modified = -1;
                    try {
                        modified = Long.parseLong(s.substring(idx + 1));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid _modified {} for {}", s.substring(idx + 1), id);
                    }
                    deletionBatch.put(id, singletonMap(KEY_MODIFIED, newEqualsCondition(modified)));
                }

                if (log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch.keySet());
                    log.debug(sb.toString());
                }

                int nRemoved = ds.remove(NODES, deletionBatch);

                if (nRemoved < deletionBatch.size()) {
                    // some nodes were re-created while GC was running
                    // find the document that still exist
                    for (String id : deletionBatch.keySet()) {
                        NodeDocument d = ds.find(NODES, id);
                        if (d != null) {
                            concurrentModification(d);
                        }
                    }
                    recreatedCount += (deletionBatch.size() - nRemoved);
                }

                deletedCount += nRemoved;
                log.debug("Deleted [{}] documents so far", deletedCount);

                if (deletedCount + recreatedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE){
                    lastLoggedCount = deletedCount + recreatedCount;
                    double progress = lastLoggedCount * 1.0 / getNumDocuments() * 100;
                    String msg = String.format("Deleted %d (%1.2f%%) documents so far", deletedCount, progress);
                    log.info(msg);
                }
            }
            return deletedCount;
        }

        private int removeDeletedPreviousDocuments() throws IOException {
            log.info("Proceeding to delete [{}] previous documents", getNumPreviousDocuments());

            int deletedCount = 0;
            int lastLoggedCount = 0;
            Iterator<List<String>> idListItr =
                    partition(getPrevDocIdsToDelete(), DELETE_BATCH_SIZE);
            while (idListItr.hasNext()) {
                List<String> deletionBatch = idListItr.next();
                deletedCount += deletionBatch.size();

                if (log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of previous documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch);
                    log.debug(sb.toString());
                }

                ds.remove(NODES, deletionBatch);

                log.debug("Deleted [{}] previous documents so far", deletedCount);

                if (deletedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE){
                    lastLoggedCount = deletedCount;
                    double progress = deletedCount * 1.0 / (prevDocIdsToDelete.getSize() - exclude.size()) * 100;
                    String msg = String.format("Deleted %d (%1.2f%%) previous documents so far", deletedCount, progress);
                    log.info(msg);
                }
            }
            return deletedCount;
        }

        private void ensureSorted() throws IOException {
            if (!sorted) {
                docIdsToDelete.sort();
                prevDocIdsToDelete.sort();
                sorted = true;
            }
        }
    }

    @Nonnull
    private StringSort newStringSort() {
        return new StringSort(overflowToDiskThreshold,
                NodeDocumentIdComparator.INSTANCE);
    }
}
