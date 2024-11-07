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

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

/**
 * This is a prototype class of a very fine-grained revision cleaner that cleans even revisions
 * in-between checkpoints. It is not clear if it will be used for now.
 *
 * A version is considered to be cleanable if it is not referenced by any checkpoint and is older than
 * a certain threshold.
 */
public class NodeDocumentRevisionCleaner {

    private final DocumentNodeStore documentNodeStore;
    private final NodeDocument workingDocument;
    private final RevisionPropertiesClassifier revisionClassifier;
    private final RevisionCleanerUtility revisionCleaner;
    private long toModifiedMs;

    /**
     * Constructor for NodeDocumentRevisionCleaner.
     * @param documentNodeStore The DocumentNodeStore instance.
     * @param workingDocument The document to clean up.
     */
    public NodeDocumentRevisionCleaner(DocumentNodeStore documentNodeStore, NodeDocument workingDocument) {
        this(documentNodeStore, workingDocument, Instant.now().minus(24, ChronoUnit.HOURS).toEpochMilli());
    }

    /**
     * Constructor for NodeDocumentRevisionCleaner.
     * @param documentNodeStore The DocumentNodeStore instance.
     * @param workingDocument The document to clean up.
     */
    public NodeDocumentRevisionCleaner(DocumentNodeStore documentNodeStore, NodeDocument workingDocument, long toModifiedMs) {
        this.workingDocument = workingDocument;
        this.documentNodeStore = documentNodeStore;
        this.toModifiedMs = toModifiedMs;

        revisionClassifier = new RevisionPropertiesClassifier(workingDocument);
        revisionCleaner = new RevisionCleanerUtility(revisionClassifier);
    }

    /**
     * Collects cleanable old revisions for the given document.
     */
    public void collectOldRevisions(UpdateOp op) {
        revisionClassifier.classifyRevisionsAndProperties();
        revisionCleaner.preserveRevisionsNewerThanThreshold(toModifiedMs);
        revisionCleaner.preserveLastRevisionForEachProperty();
        revisionCleaner.preserveRevisionsReferencedByCheckpoints();
        revisionCleaner.removeCandidatesInList();

        for (Map.Entry<Integer, TreeSet<Revision>> entry : revisionCleaner.getCandidateRevisionsToClean().entrySet()) {
            for (Revision revision : entry.getValue()) {
                TreeSet<String> properties = revisionClassifier.getPropertiesModifiedByRevision().get(revision);
                if (properties != null) {
                    outer:for (String property : properties) {
                        Map<Key, Operation> c = op.getChanges();
                        for (Entry<Key, Operation> e : c.entrySet()) {
                            if (e.getKey().equals(new Key(property, null)) && e.getValue().type == Type.REMOVE) {
                                continue outer;
                            }
                        }
                        op.removeMapEntry(property, revision);
                    }
                }
                RevisionVector sweepRevisions = documentNodeStore.getSweepRevisions();
                boolean newerThanSweep = sweepRevisions == null ? false : sweepRevisions.isRevisionNewer(revision);
                boolean isBC = workingDocument.getLocalBranchCommits().contains(revision);
                if (!newerThanSweep && !isBC) {
                    op.removeMapEntry("_revisions", revision);
                }
            }
        }
    }

    private class RevisionPropertiesClassifier {
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
        private void classifyRevisionsAndProperties() {
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

        public SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> getRevisionsModifyingPropertyByCluster() {
            return revisionsModifyingPropertyByCluster;
        }

        public SortedMap<String, TreeSet<Revision>> getRevisionsModifyingProperty() {
            return revisionsModifyingProperty;
        }

        public SortedMap<Revision, TreeSet<String>> getPropertiesModifiedByRevision() {
            return propertiesModifiedByRevision;
        }
    }

    private class RevisionCleanerUtility {

        private final SortedMap<Integer, TreeSet<Revision>> blockedRevisionsToKeep;
        private final SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean;
        private final RevisionPropertiesClassifier revisionClassifier;

        private RevisionCleanerUtility(RevisionPropertiesClassifier revisionClassifier) {
            this.revisionClassifier = revisionClassifier;
            this.candidateRevisionsToClean = new TreeMap<>();
            this.blockedRevisionsToKeep = new TreeMap<>();
        }

        private void preserveLastRevisionForEachProperty() {
            for (SortedMap<Integer, TreeSet<Revision>> revisionsByCluster : revisionClassifier.getRevisionsModifyingPropertyByCluster().values()) {
                for (TreeSet<Revision> revisions : revisionsByCluster.values()) {
                    Revision lastRevision = revisions.last();
                    addBlockedRevisionToKeep(lastRevision);
                }
            }
        }

        private void preserveRevisionsNewerThanThreshold(long amount, ChronoUnit unit) {
            long thresholdToPreserve = Instant.now().minus(amount, unit).toEpochMilli();
            preserveRevisionsNewerThanThreshold(thresholdToPreserve);
        }

        private void preserveRevisionsNewerThanThreshold(long thresholdToPreserve) {
            for (TreeSet<Revision> revisionSet : candidateRevisionsToClean.values()) {
                for (Revision revision : revisionSet) {
                    if (revision.getTimestamp() > thresholdToPreserve) {
                        addBlockedRevisionToKeep(revision);
                    }
                }
            }
        }

        private void preserveRevisionsReferencedByCheckpoints() {
            SortedMap<Revision, Checkpoints.Info> checkpoints = documentNodeStore.getCheckpoints().getCheckpoints();
            checkpoints.forEach((revision, info) -> {
                // For each checkpoint, keep the last revision that modified a property prior to checkpoint
                revisionClassifier.getRevisionsModifyingProperty().forEach((propertyName, revisionsSet) -> {
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

        private void removeCandidatesInList() {
            revisionCleaner.blockedRevisionsToKeep.forEach((key, value) -> {
                if (revisionCleaner.getCandidateRevisionsToClean().containsKey(key)) {
                    revisionCleaner.getCandidateRevisionsToClean().get(key).removeAll(value);
                }
            });
        }

        public SortedMap<Integer, TreeSet<Revision>> getBlockedRevisionsToKeep() {
            return blockedRevisionsToKeep;
        }

        public SortedMap<Integer, TreeSet<Revision>> getCandidateRevisionsToClean() {
            return candidateRevisionsToClean;
        }
    }

    /*
     * The following methods are used to expose the internal state of the cleaner for testing/debugging purposes.
     */
    protected SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> getRevisionsModifyingPropertyByCluster() {
        return revisionClassifier.getRevisionsModifyingPropertyByCluster();
    }

    protected SortedMap<String, TreeSet<Revision>> getRevisionsModifyingProperty() {
        return revisionClassifier.getRevisionsModifyingProperty();
    }

    protected SortedMap<Revision, TreeSet<String>> getPropertiesModifiedByRevision() {
        return revisionClassifier.getPropertiesModifiedByRevision();
    }

    public SortedMap<Integer, TreeSet<Revision>> getBlockedRevisionsToKeep() {
        return revisionCleaner.getBlockedRevisionsToKeep();
    }

    public SortedMap<Integer, TreeSet<Revision>> getCandidateRevisionsToClean() {
        return revisionCleaner.getCandidateRevisionsToClean();
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
