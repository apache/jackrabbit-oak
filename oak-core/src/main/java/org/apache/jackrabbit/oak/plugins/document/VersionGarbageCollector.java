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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterators.partition;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;

public class VersionGarbageCollector {
    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private final DocumentNodeStore nodeStore;
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
    }

    public VersionGCStats gc(long maxRevisionAge, TimeUnit unit) throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        Stopwatch sw = Stopwatch.createStarted();
        VersionGCStats stats = new VersionGCStats();
        final long oldestRevTimeStamp = nodeStore.getClock().getTime() - maxRevisionAgeInMillis;
        final Revision headRevision = nodeStore.getHeadRevision();

        log.info("Starting revision garbage collection. Revisions older than [{}] would be " +
                "removed", Utils.timestampToString(oldestRevTimeStamp));

        //Check for any registered checkpoint which prevent the GC from running
        Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
        if (checkpoint != null && checkpoint.getTimestamp() < oldestRevTimeStamp) {
            log.info("Ignoring version gc as valid checkpoint [{}] found while " +
                            "need to collect versions older than [{}]", checkpoint.toReadableString(),
                    Utils.timestampToString(oldestRevTimeStamp)
            );
            stats.ignoredGCDueToCheckPoint = true;
            return stats;
        }

        collectDeletedDocuments(stats, headRevision, oldestRevTimeStamp);
        collectSplitDocuments(stats, oldestRevTimeStamp);

        sw.stop();
        log.info("Version garbage collected in {}. {}", sw, stats);
        return stats;
    }

    public void setOverflowToDiskThreshold(int overflowToDiskThreshold) {
        this.overflowToDiskThreshold = overflowToDiskThreshold;
    }

    private void collectSplitDocuments(VersionGCStats stats, long oldestRevTimeStamp) {
        versionStore.deleteSplitDocuments(GC_TYPES, oldestRevTimeStamp, stats);
    }

    private void collectDeletedDocuments(VersionGCStats stats, Revision headRevision, long oldestRevTimeStamp)
            throws IOException {
        StringSort docIdsToDelete = new StringSort(overflowToDiskThreshold, NodeDocumentIdComparator.INSTANCE);
        try {
            stats.collectDeletedDocs.start();
            Iterable<NodeDocument> itr = versionStore.getPossiblyDeletedDocs(oldestRevTimeStamp);
            try {
                for (NodeDocument doc : itr) {
                    //Check if node is actually deleted at current revision
                    //As node is not modified since oldestRevTimeStamp then
                    //this node has not be revived again in past maxRevisionAge
                    //So deleting it is safe
                    if (doc.getNodeAtRevision(nodeStore, headRevision, null) == null) {
                        docIdsToDelete.add(doc.getId());
                        //Collect id of all previous docs also
                        for (NodeDocument prevDoc : ImmutableList.copyOf(doc.getAllPreviousDocs())) {
                            docIdsToDelete.add(prevDoc.getId());
                        }
                    }
                }
            } finally {
                Utils.closeIfCloseable(itr);
            }
            stats.collectDeletedDocs.stop();

            if (docIdsToDelete.isEmpty()){
                return;
            }

            docIdsToDelete.sort();
            log.info("Proceeding to delete [{}] documents", docIdsToDelete.getSize());

            stats.deleteDeletedDocs.start();
            Iterator<List<String>> idListItr = partition(docIdsToDelete.getIds(), DELETE_BATCH_SIZE);
            int deletedCount = 0;
            while (idListItr.hasNext()) {
                List<String> deletionBatch = idListItr.next();
                deletedCount += deletionBatch.size();

                if (log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of documents with following ids. \n");
                    Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).appendTo(sb, deletionBatch);
                    log.debug(sb.toString());
                }
                log.debug("Deleted [{}] documents so far", deletedCount);

                nodeStore.getDocumentStore().remove(Collection.NODES, deletionBatch);
            }

            nodeStore.invalidateDocChildrenCache();
            stats.deleteDeletedDocs.stop();
            stats.deletedDocGCCount += docIdsToDelete.getSize();
        } finally {
            docIdsToDelete.close();
        }
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        int deletedDocGCCount;
        int splitDocGCCount;
        int intermediateSplitDocGCCount;
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();


        @Override
        public String toString() {
            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    ", deletedDocGCCount=" + deletedDocGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", timeToCollectDeletedDocs=" + collectDeletedDocs +
                    ", timeTakenToDeleteDocs=" + deleteDeletedDocs +
                    '}';
        }
    }
}
