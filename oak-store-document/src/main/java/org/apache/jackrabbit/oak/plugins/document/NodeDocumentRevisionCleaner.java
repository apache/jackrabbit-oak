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

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

public class NodeDocumentRevisionCleaner {

    private final DocumentStore documentStore;
    private final DocumentNodeStore documentNodeStore;
    private final NodeDocument workingDocument;

    protected RevisionClassifierUtility revisionClassifier;
    protected RevisionCleanerUtility revisionCleaner;

    /**
     * Constructor for DocumentRevisionCleanupHelper.
     * @param documentStore The DocumentStore instance. Must be writable to perform cleanup.
     * @param documentNodeStore The DocumentNodeStore instance.
     * @param path The path of the document to clean up.
     */
    public NodeDocumentRevisionCleaner(DocumentStore documentStore, DocumentNodeStore documentNodeStore, String path) {
        String id = Utils.getIdFromPath(path);
        this.workingDocument = documentStore.find(NODES, id);
        this.documentStore = documentStore;
        this.documentNodeStore = documentNodeStore;

        revisionClassifier = new RevisionClassifierUtility(workingDocument);
        revisionCleaner = new RevisionCleanerUtility(revisionClassifier);
    }

    /**
     * Performs the full revision cleanup process for the given document for a clusterId.
     */
    public void initializeCleanupProcess() {
        revisionClassifier.identifyRevisionsToClean();
        revisionCleaner.markRevisionsNewerThanThresholdToPreserve(24, ChronoUnit.HOURS);
        revisionCleaner.markLastRevisionForEachProperty();
        revisionCleaner.markCheckpointRevisionsToPreserve();
        revisionCleaner.removeCandidatesInList();
    }

    public int executeCleanupProcess(int numberToCleanup, int clusterToCleanup) {
        return -99;
    }

    protected void classifyAndMapRevisionsAndProperties() {
        //identifyRevisionsToClean();
        //BclassifyAndMapRevisionsAndProperties();

        revisionClassifier.identifyRevisionsToClean();
        //newClassifyAndMapRevisionsAndProperties();
    }

    /*protected void newClassifyAndMapRevisionsAndProperties() {
        candidateRevisionsToClean = new TreeMap<>();
        blockedRevisionsToKeep = new TreeMap<>();
        revisionsModifyingPropertyByCluster = new TreeMap<>();
        revisionsModifyingProperty = new TreeMap<>();
        propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);

        // The first entry in "_deleted" has to be kept, as is when the document was created
        //NavigableMap<Revision, String> deletedRevisions = ((NavigableMap<Revision, String>) workingDocument.get("_deleted"));
        SortedMap<Revision, String> deletedRevisions = workingDocument.getLocalDeleted();
        if (deletedRevisions != null && !deletedRevisions.isEmpty()) {
            Revision createdRevision = deletedRevisions.firstKey();
            addBlockedRevisionToKeep(createdRevision);
        }

        SortedMap<Revision, String> documentRevisions = getAllRevisions();
        for (Map.Entry<Revision, String> revisionEntry : documentRevisions.entrySet()) {
            Revision revision = revisionEntry.getKey();
            String revisionValue = revisionEntry.getValue();

            // Only check committed revisions (ignore branch commits starting with "c-")
            if ("c".equals(revisionValue)) {
                // Candidate to clean up
                addCandidateRevisionToClean(revision);
                // Store properties usage
                mapPropertiesModifiedByThisRevision(revision);
            }
        }
    }*/

    /**
     * Step 1:
     * This method processes the revisions of the working document, classifying them into two categories:
     * candidate revisions that can be cleaned up and used revisions that should be kept. It also creates maps to
     * track the relationships between revisions and properties modified by them.
     */
    /*protected void BclassifyAndMapRevisionsAndProperties() {
        candidateRevisionsToClean = new TreeMap<>();
        blockedRevisionsToKeep = new TreeMap<>();
        revisionsModifyingPropertyByCluster = new TreeMap<>();
        revisionsModifyingProperty = new TreeMap<>();
        propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);

        // The first entry in "_deleted" has to be kept, as is when the document was created
        //NavigableMap<Revision, String> deletedRevisions = ((NavigableMap<Revision, String>) workingDocument.get("_deleted"));
        NavigableMap<Revision, String> deletedRevisions = ((NavigableMap<Revision, String>) workingDocument.getLocalDeleted());
        if (deletedRevisions != null && !deletedRevisions.isEmpty()) {
            // TODO: This is just a check to ensure the order is the expected
            assert(deletedRevisions.descendingMap().lastKey().getTimestamp() <= deletedRevisions.descendingMap().firstKey().getTimestamp());

            Revision createdRevision = deletedRevisions.descendingMap().lastKey();
            addBlockedRevisionToKeep(createdRevision);
        }

        SortedMap<Revision, String> documentRevisions = getAllRevisions();
        for (Map.Entry<Revision, String> revisionEntry : documentRevisions.entrySet()) {
            Revision revision = revisionEntry.getKey();
            String revisionValue = revisionEntry.getValue();

            // Only check committed revisions (ignore branch commits starting with "c-")
            if ("c".equals(revisionValue)) {
                // Candidate to clean up
                addCandidateRevisionToClean(revision);
                // Store properties usage
                mapPropertiesModifiedByThisRevision(revision);
            }
        }
    }*/

    protected class RevisionClassifierUtility {
        private final NodeDocument workingDocument;
        private final SortedMap<Revision, String> documentRevisions;
        private SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingPropertyByCluster;
        private SortedMap<String, TreeSet<Revision>> revisionsModifyingProperty;
        private SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision;

        RevisionClassifierUtility(NodeDocument workingDocument) {
            this.workingDocument = workingDocument;
            this.documentRevisions = workingDocument.getLocalRevisions();

            this.revisionsModifyingPropertyByCluster = new TreeMap<>();
            this.revisionsModifyingProperty = new TreeMap<>();
            this.propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);
        }

        public void identifyRevisionsToClean() {
            SortedMap<Revision, String> deletedRevisions = workingDocument.getLocalDeleted();
            // Always keep the first "_deleted" entry, as is when the document was created
            if (!deletedRevisions.isEmpty()) {
                Revision createdRevision = deletedRevisions.firstKey();
                revisionCleaner.addBlockedRevisionToKeep(createdRevision);
            }

            SortedMap<Revision, String> documentRevisions = workingDocument.getLocalRevisions();
            for (Map.Entry<Revision, String> revisionEntry : documentRevisions.entrySet()) {
                Revision revision = revisionEntry.getKey();
                String revisionValue = revisionEntry.getValue();

                // Only check committed revisions (ignore branch commits starting with "c-")
                if (Utils.isCommitted(revisionValue)) {
                    // Candidate to clean up
                    revisionCleaner.addCandidateRevisionToClean(revision);
                    // Store properties usage
                    mapPropertiesModifiedByThisRevision(revision);
                }
            }
        }

        private void mapPropertiesModifiedByThisRevision(Revision revision) {
            for (Map.Entry<String, Object> propertyEntry : workingDocument.entrySet()) {
                if (Utils.isPropertyName(propertyEntry.getKey()) || NodeDocument.isDeletedEntry(propertyEntry.getKey())) {
                    Map<Revision, String> valueMap = (Map) propertyEntry.getValue();
                    if (valueMap.containsKey(revision)) {
                        propertiesModifiedByRevision.computeIfAbsent(revision, key ->
                                new TreeSet<>()).add(propertyEntry.getKey()
                        );

                        revisionsModifyingPropertyByCluster.computeIfAbsent(propertyEntry.getKey(), key ->
                                new TreeMap<>()
                        ).computeIfAbsent(revision.getClusterId(), key ->
                                new TreeSet<>(StableRevisionComparator.INSTANCE)
                        ).add(revision);

                        revisionsModifyingProperty.computeIfAbsent(propertyEntry.getKey(), key ->
                                new TreeSet<>(StableRevisionComparator.INSTANCE)
                        ).add(revision);
                    }
                }
            }
        }
    }

    protected class RevisionCleanerUtility {

        private SortedMap<Integer, TreeSet<Revision>> blockedRevisionsToKeep;
        private SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean;
        private final RevisionClassifierUtility revisionClassifier;

        protected RevisionCleanerUtility(RevisionClassifierUtility revisionClassifier) {
            this.revisionClassifier = revisionClassifier;
            this.candidateRevisionsToClean = new TreeMap<>();
            this.blockedRevisionsToKeep = new TreeMap<>();
        }

        protected void markLastRevisionForEachProperty() {
            for (SortedMap<Integer, TreeSet<Revision>> revisionsByCluster : revisionClassifier.revisionsModifyingPropertyByCluster.values()) {
                for (TreeSet<Revision> revisions : revisionsByCluster.values()) {
                    Revision lastRevision = revisions.last();
                    addBlockedRevisionToKeep(lastRevision);
                }
            }
        }

        protected void markRevisionsNewerThanThresholdToPreserve(long amount, ChronoUnit unit) {
            long thresholdToPreserve = Instant.now().minus(amount, unit).toEpochMilli();
            for (TreeSet<Revision> revisionSet : candidateRevisionsToClean.values()) {
                for (Revision revision : revisionSet) {
                    if (revision.getTimestamp() > thresholdToPreserve) {
                        addBlockedRevisionToKeep(revision);
                    }
                }
            }
        }

        protected void markCheckpointRevisionsToPreserve() {
            SortedMap<Revision, Checkpoints.Info> checkpoints = documentNodeStore.getCheckpoints().getCheckpoints();
            checkpoints.forEach((revision, info) -> {
                // For each checkpoint, keep the last revision that modified a property prior to checkpoint
                revisionClassifier.revisionsModifyingProperty.forEach((propertyName, revisionsSet) -> {
                    // Traverse the revisionVector of the checkpoint and find the last revision that modified the property
                    info.getCheckpoint().forEach(revisionToFind -> {
                        // If the exact revision exists, keep it. If not, find the previous one that modified that property
                        if (revisionsSet.contains(revisionToFind)) {
                            addBlockedRevisionToKeep(revisionToFind);
                        } else {
                            Revision previousRevision = revisionsSet.descendingSet().ceiling(revisionToFind);
                            if (previousRevision != null) {
                                addBlockedRevisionToKeep(previousRevision);
                            }
                        }
                    });
                });
            });
        }

        /**
         * Adds a revision to the list of candidates to delete.
         * @param revision
         */
        private void addCandidateRevisionToClean(Revision revision) {
            candidateRevisionsToClean.computeIfAbsent(revision.getClusterId(), key ->
                    new TreeSet<>(StableRevisionComparator.INSTANCE)
            ).add(revision);
        }

        /**
         * Adds a revision to the list of revisions to keep.
         * @param revision
         */
        private void addBlockedRevisionToKeep(Revision revision) {
            blockedRevisionsToKeep.computeIfAbsent(revision.getClusterId(), key ->
                    new TreeSet<>(StableRevisionComparator.INSTANCE)
            ).add(revision);
        }

        protected void removeCandidatesInList() {
            revisionCleaner.blockedRevisionsToKeep.forEach((key, value) -> {
                if (revisionCleaner.candidateRevisionsToClean.containsKey(key)) {
                    revisionCleaner.candidateRevisionsToClean.get(key).removeAll(value);
                }
            });
        }
    }

    public NavigableMap<Revision, String> getAllRevisions() {
        return (NavigableMap<Revision, String>) workingDocument.getLocalRevisions();
    }

    public SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> getRevisionsModifyingPropertyByCluster() {
        return revisionClassifier.revisionsModifyingPropertyByCluster;
    }

    public SortedMap<String, TreeSet<Revision>> getRevisionsModifyingProperty() {
        return revisionClassifier.revisionsModifyingProperty;
    }

    public SortedMap<Revision, TreeSet<String>> getPropertiesModifiedByRevision() {
        return revisionClassifier.propertiesModifiedByRevision;
    }

    public SortedMap<Integer, TreeSet<Revision>> getBlockedRevisionsToKeep() {
        return revisionCleaner.blockedRevisionsToKeep;
    }

    public SortedMap<Integer, TreeSet<Revision>> getCandidateRevisionsToClean() {
        return revisionCleaner.candidateRevisionsToClean;
    }
}
