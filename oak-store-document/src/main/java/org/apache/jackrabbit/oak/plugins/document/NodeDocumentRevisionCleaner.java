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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class NodeDocumentRevisionCleaner {

    private final DocumentStore documentStore;
    private final DocumentNodeStore documentNodeStore;
    private final NodeDocument workingDocument;
    private final RevisionPropertiesClassifier revisionClassifier;
    private final RevisionCleanerUtility revisionCleaner;

    /**
     * Constructor for NodeDocumentRevisionCleaner.
     * @param documentStore The DocumentStore instance. Must be writable to perform cleanup.
     * @param documentNodeStore The DocumentNodeStore instance.
     * @param workingDocument The document to clean up.
     */
    public NodeDocumentRevisionCleaner(DocumentStore documentStore, DocumentNodeStore documentNodeStore,
                                       NodeDocument workingDocument) {
        this.workingDocument = workingDocument;
        this.documentStore = documentStore;
        this.documentNodeStore = documentNodeStore;

        revisionClassifier = new RevisionPropertiesClassifier(workingDocument);
        revisionCleaner = new RevisionCleanerUtility(revisionClassifier);
    }

    /**
     * Collects cleanable old revisions for the given document.
     */
    public void collectOldRevisions(UpdateOp op) {
        revisionClassifier.classifyRevisionsAndProperties();
        revisionCleaner.preserveRevisionsNewerThanThreshold(24, ChronoUnit.HOURS);
        revisionCleaner.preserveLastRevisionForEachProperty();
        revisionCleaner.preserveRevisionsReferencedByCheckpoints();
        revisionCleaner.removeCandidatesInList();

        /*for (Map.Entry<Integer, TreeSet<Revision>> entry : revisionCleaner.candidateRevisionsToClean.entrySet()) {
            for (Revision revision : entry.getValue()) {
                System.out.println("Removing revision " + revision);
                TreeSet<String> properties = revisionClassifier.propertiesModifiedByRevision.get(revision);
                System.out.println("Properties modified by this revision: " + properties);
                if (properties != null) {
                    for (String property : properties) {
                        op.removeMapEntry(property, revision);
                    }
                }

                op.removeMapEntry("_revisions", revision);
            }
        }*/
    }

    protected class RevisionPropertiesClassifier {
        private final NodeDocument workingDocument;
        private SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingPropertyByCluster;
        private SortedMap<String, TreeSet<Revision>> revisionsModifyingProperty;
        private SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision;

        private RevisionPropertiesClassifier(NodeDocument workingDocument) {
            this.workingDocument = workingDocument;

            this.revisionsModifyingPropertyByCluster = new TreeMap<>();
            this.revisionsModifyingProperty = new TreeMap<>();
            this.propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);
        }

        /**
         * This method processes the revisions of the working document, creating maps to
         * track the relationships between revisions and modified properties.
         */
        protected void classifyRevisionsAndProperties() {
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

                // Only check committed revisions
                if (Utils.isCommitted(revisionValue)) {
                    // Candidate to clean up
                    revisionCleaner.addCandidateRevisionToClean(revision);
                    // Store properties usage
                    classifyPropertiesModifiedByRevision(revision);
                }
            }
        }

        private void classifyPropertiesModifiedByRevision(Revision revision) {
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

        private final SortedMap<Integer, TreeSet<Revision>> blockedRevisionsToKeep;
        private final SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean;
        private final RevisionPropertiesClassifier revisionClassifier;

        private RevisionCleanerUtility(RevisionPropertiesClassifier revisionClassifier) {
            this.revisionClassifier = revisionClassifier;
            this.candidateRevisionsToClean = new TreeMap<>();
            this.blockedRevisionsToKeep = new TreeMap<>();
        }

        protected void preserveLastRevisionForEachProperty() {
            for (SortedMap<Integer, TreeSet<Revision>> revisionsByCluster : revisionClassifier.revisionsModifyingPropertyByCluster.values()) {
                for (TreeSet<Revision> revisions : revisionsByCluster.values()) {
                    Revision lastRevision = revisions.last();
                    addBlockedRevisionToKeep(lastRevision);
                }
            }
        }

        protected void preserveRevisionsNewerThanThreshold(long amount, ChronoUnit unit) {
            long thresholdToPreserve = Instant.now().minus(amount, unit).toEpochMilli();
            for (TreeSet<Revision> revisionSet : candidateRevisionsToClean.values()) {
                for (Revision revision : revisionSet) {
                    if (revision.getTimestamp() > thresholdToPreserve) {
                        addBlockedRevisionToKeep(revision);
                    }
                }
            }
        }

        protected void preserveRevisionsReferencedByCheckpoints() {
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

    public SortedMap<Revision, String> getAllRevisions() {
        return workingDocument.getLocalRevisions();
    }

    protected SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> getRevisionsModifyingPropertyByCluster() {
        return revisionClassifier.revisionsModifyingPropertyByCluster;
    }

    protected SortedMap<String, TreeSet<Revision>> getRevisionsModifyingProperty() {
        return revisionClassifier.revisionsModifyingProperty;
    }

    protected SortedMap<Revision, TreeSet<String>> getPropertiesModifiedByRevision() {
        return revisionClassifier.propertiesModifiedByRevision;
    }

    public SortedMap<Integer, TreeSet<Revision>> getBlockedRevisionsToKeep() {
        return revisionCleaner.blockedRevisionsToKeep;
    }

    public SortedMap<Integer, TreeSet<Revision>> getCandidateRevisionsToClean() {
        return revisionCleaner.candidateRevisionsToClean;
    }

    protected void classifyRevisionsAndProperties() {
        revisionClassifier.classifyRevisionsAndProperties();
    }

    protected void markLastRevisionForEachProperty() {
        revisionCleaner.preserveLastRevisionForEachProperty();
    }

    protected void markRevisionsNewerThanThresholdToPreserve(long amount, ChronoUnit unit) {
        revisionCleaner.preserveRevisionsNewerThanThreshold(amount, unit);
    }

    protected void markCheckpointRevisionsToPreserve() {
        revisionCleaner.preserveRevisionsReferencedByCheckpoints();
    }

    protected void removeCandidatesInList() {
        revisionCleaner.removeCandidatesInList();
    }
}
