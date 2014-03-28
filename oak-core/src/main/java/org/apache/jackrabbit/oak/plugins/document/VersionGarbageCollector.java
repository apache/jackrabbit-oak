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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VersionGarbageCollector {
    private final DocumentNodeStore nodeStore;
    private final VersionGCSupport versionStore;

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Split document types which can be safely Garbage Collected
     */
    private static final Set<NodeDocument.SplitDocType> GC_TYPES = EnumSet.of(
            NodeDocument.SplitDocType.DEFAULT_NO_CHILD,
            NodeDocument.SplitDocType.PROP_COMMIT_ONLY,
            NodeDocument.SplitDocType.INTERMEDIATE);

    private volatile long maxRevisionAge = TimeUnit.DAYS.toMillis(1);

    VersionGarbageCollector(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;

        if(nodeStore.getDocumentStore() instanceof MongoDocumentStore){
            this.versionStore =
                    new MongoVersionGCSupport((MongoDocumentStore) nodeStore.getDocumentStore());
        }else {
            this.versionStore = new VersionGCSupport(nodeStore.getDocumentStore());
        }
    }

    public VersionGCStats gc() {
        VersionGCStats stats = new VersionGCStats();
        final long oldestRevTimeStamp = nodeStore.getClock().getTime() - maxRevisionAge;
        final Revision headRevision = nodeStore.getHeadRevision();

        //Check for any registered checkpoint which prevent the GC from running
        Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
        if (checkpoint != null && checkpoint.getTimestamp() < oldestRevTimeStamp) {
            log.info("Ignoring version gc as valid checkpoint [{}] found while " +
                            "need to collect versions older than [{}]", checkpoint.toReadableString(),
                    Revision.timestampToString(oldestRevTimeStamp)
            );
            stats.ignoredGCDueToCheckPoint = true;
            return stats;
        }

        collectDeletedDocuments(stats, headRevision, oldestRevTimeStamp);
        collectSplitDocuments(stats, oldestRevTimeStamp);

        return stats;
    }

    private void collectSplitDocuments(VersionGCStats stats, long oldestRevTimeStamp) {
        int count = versionStore.deleteSplitDocuments(GC_TYPES, oldestRevTimeStamp);
        stats.splitDocGCCount += count;
    }

    private void collectDeletedDocuments(VersionGCStats stats, Revision headRevision, long oldestRevTimeStamp) {
        List<String> docIdsToDelete = new ArrayList<String>();
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
            close(itr);
        }
        nodeStore.getDocumentStore().remove(Collection.NODES, docIdsToDelete);
        stats.deletedDocGCCount += docIdsToDelete.size();
    }

    public void setMaxRevisionAge(long maxRevisionAge) {
        this.maxRevisionAge = maxRevisionAge;
    }

    public long getMaxRevisionAge() {
        return maxRevisionAge;
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        int deletedDocGCCount;
        int splitDocGCCount;
    }

    private void close(Object obj){
        if(obj instanceof Closeable){
           try{
               ((Closeable) obj).close();
           } catch (IOException e) {
                log.warn("Error occurred while closing", e);
           }
        }
    }
}
