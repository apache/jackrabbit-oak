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

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;

public class DocumentRevisionCleanupHelper {

    private final DocumentStore documentStore;
    private final DocumentNodeStore documentNodeStore;
    private final NodeDocument rootDoc;
    private final NodeDocument workingDocument;

    private SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty;
    private SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision;
    private SortedMap<Integer, TreeSet<Revision>> blockedRevisionsToKeep;
    private SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean;

    /**
     * Constructor for DocumentRevisionCleanupHelper.
     * @param documentStore The DocumentStore instance. Must be writable to perform cleanup.
     * @param documentNodeStore The DocumentNodeStore instance.
     * @param path The path of the document to clean up.
     */
    public DocumentRevisionCleanupHelper(DocumentStore documentStore, DocumentNodeStore documentNodeStore, String path) {
        this.candidateRevisionsToClean = new TreeMap<>();
        this.blockedRevisionsToKeep = new TreeMap<>();
        this.revisionsModifyingProperty = new TreeMap<>();
        this.propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);

        this.rootDoc = documentStore.find(NODES, Utils.getIdFromPath("/"));
        String id = Utils.getIdFromPath(path);
        this.workingDocument = documentStore.find(NODES, id);
        this.documentStore = documentStore;
        this.documentNodeStore = documentNodeStore;
    }

    /**
     * Performs the full revision cleanup process for the given document for a clusterId.
     */
    public void initializeCleanupProcess() {
        classifyAndMapRevisionsAndProperties();
        markRevisionsNewerThanThresholdToPreserve(24, ChronoUnit.HOURS);
        markLastRevisionForEachProperty();
        markCheckpointRevisionsToPreserve();
        removeCandidatesInList(blockedRevisionsToKeep);
    }

    public int executeCleanupProcess(int numberToCleanup, int clusterToCleanup) {
        return -99;
    }

    /**
     * Step 1:
     * This method processes the revisions of the working document, classifying them into two categories:
     * candidate revisions that can be cleaned up and used revisions that should be kept. It also creates maps to
     * track the relationships between revisions and properties modified by them.
     */
    protected void classifyAndMapRevisionsAndProperties() {
        candidateRevisionsToClean = new TreeMap<>();
        blockedRevisionsToKeep = new TreeMap<>();
        revisionsModifyingProperty = new TreeMap<>();
        propertiesModifiedByRevision = new TreeMap<>(StableRevisionComparator.INSTANCE);

        // The first entry in "_deleted" has to be kept, as is when the document was created
        NavigableMap<Revision, String> deletedRevisions = ((NavigableMap<Revision, String>) workingDocument.get("_deleted"));
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
    }

    /**
     * Step 2:
     * This method filters out revisions that are newer than a specified time threshold (specified by amount and unit)
     * from the candidate revisions to be cleaned up, marking them as used revisions that should be kept.
     * @param amount the amount of time
     * @param unit the unit of time
     */
    protected void markRevisionsNewerThanThresholdToPreserve(long amount, ChronoUnit unit) {
        // TODO: Should we add a buffer here? Maybe 1 minute or even 1 hour?
        long thresholdToPreserve = Instant.now().minus(amount, unit).toEpochMilli();
        for (TreeSet<Revision> revisionSet : candidateRevisionsToClean.values()) {
            for (Revision revision : revisionSet) {
                if (revision.getTimestamp() > thresholdToPreserve) {
                    addBlockedRevisionToKeep(revision);
                }
            }
        }
    }

    /**
     * Step 3:
     * This method processes a set of revisions that modified certain properties and keeps the last revision that
     * modified each property. This means, the current status of the node will be preserved.
     */
    protected void markLastRevisionForEachProperty() {
        for (SortedMap<Integer, TreeSet<Revision>> revisionsByCluster : revisionsModifyingProperty.values()) {
            for (TreeSet<Revision> revisions : revisionsByCluster.values()) {
                Revision lastRevision = revisions.last();
                addBlockedRevisionToKeep(lastRevision);
            }
        }
    }

    /**
     * Step 4:
     * Process a set of revisions that modified certain properties and determine which revisions should be
     * kept based on the checkpoints.
     */
    protected void markCheckpointRevisionsToPreserve() {
        SortedMap<Revision, Checkpoints.Info> checkpoints = documentNodeStore.getCheckpoints().getCheckpoints();
        checkpoints.forEach((revision, info) -> {
            // For each checkpoint, keep the last revision that modified a property prior to checkpoint
            revisionsModifyingProperty.forEach((propertyName, revisionsByCluster) -> {
                // Traverse the revisionVector of the checkpoint and find the last revision that modified the property
                info.getCheckpoint().forEach(revisionToFind -> {
                    TreeSet<Revision> listOfRevisionsForProperty = revisionsByCluster.get(revisionToFind.getClusterId());
                    if (listOfRevisionsForProperty != null) {
                        // If the exact revision exists, keep it. If not, find the previous one that modified that property
                        if (listOfRevisionsForProperty.contains(revisionToFind)) {
                            addBlockedRevisionToKeep(revisionToFind);
                        } else {
                            Revision previousRevision = listOfRevisionsForProperty.descendingSet().ceiling(revisionToFind);
                            if (previousRevision != null) {
                                addBlockedRevisionToKeep(previousRevision);
                            }
                        }
                    }
                });
            });
        });
    }

    /**
     * Step 5:
     * Removes for each clusterId the revisions in the Map, that were blocked in the methods above.
     */
    protected void removeCandidatesInList(SortedMap<Integer, TreeSet<Revision>> revisions) {
        revisions.forEach((key, value) -> {
            if (candidateRevisionsToClean.containsKey(key)) {
                candidateRevisionsToClean.get(key).removeAll(value);
            }
        });
    }

    /**
     * This method processes a given revision and identify the properties modified by it within the working document.
     * It maintains two data structures, propertiesModifiedByRevision and revisionsModifyingProperty, to store
     * the properties and their associated revisions.
     * @param revision
     */
    private void mapPropertiesModifiedByThisRevision(Revision revision) {
        for (Map.Entry<String, Object> propertyEntry : workingDocument.entrySet()) {
            if (Utils.isPropertyName(propertyEntry.getKey()) || isDeletedEntry(propertyEntry.getKey())) {
                Map<Revision, String> valueMap = (Map) propertyEntry.getValue();
                if (valueMap.containsKey(revision)) {
                    propertiesModifiedByRevision.computeIfAbsent(revision, key ->
                            new TreeSet<>()).add(propertyEntry.getKey()
                    );

                    revisionsModifyingProperty.computeIfAbsent(propertyEntry.getKey(), key ->
                            new TreeMap<>()
                    ).computeIfAbsent(revision.getClusterId(), key ->
                            new TreeSet<>(StableRevisionComparator.INSTANCE)
                    ).add(revision);
                }
            }
        }
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

    /**
     * Returns the LastRev map from the root document.
     * @return
     */
    public Map<Integer, Revision> getLastRev() {
        return rootDoc.getLastRev();
    }

    /**
     * Returns the SweepRev map from the root document.
     * @return
     */
    public Map<Integer, Revision> getSweepRev() {
        Map<Integer, Revision> map = Maps.newHashMap();
        Map<Revision, String> valueMap = (SortedMap<Revision, String>) rootDoc.get("_sweepRev");
        for (Map.Entry<Revision, String> e : valueMap.entrySet()) {
            int clusterId = e.getKey().getClusterId();
            Revision rev = Revision.fromString(e.getValue());
            map.put(clusterId, rev);
        }
        return map;
    }

    public NavigableMap<Revision, String> getAllRevisions() {
        return (NavigableMap<Revision, String>) workingDocument.get("_revisions");
    }

    public SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> getRevisionsModifyingProperty() {
        return revisionsModifyingProperty;
    }

    public SortedMap<Revision, TreeSet<String>> getPropertiesModifiedByRevision() {
        return propertiesModifiedByRevision;
    }

    public SortedMap<Integer, TreeSet<Revision>> getBlockedRevisionsToKeep() {
        return blockedRevisionsToKeep;
    }

    public SortedMap<Integer, TreeSet<Revision>> getCandidateRevisionsToClean() {
        return candidateRevisionsToClean;
    }
}
