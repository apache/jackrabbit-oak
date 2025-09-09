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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.GCPhase;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;

/**
 * Collector interface for {@link DocumentNodeStore} full garbage collection
 * statistics.
 */
public interface FullGCStatsCollector {

    /**
     * Total No. of documents read during FullGC phase
     */
    void documentRead();

    /**
     * Total No. of properties detected as garbage during a given GC phase
     * @param mode GC phase
     * @param numProps no. of garbage properties found in current cycle
     */
    void candidateProperties(GCPhase mode, long numProps);

    /**
     * Total No. of documents detected as garbage during a given GC phase
     * @param mode GC phase
     * @param numDocs no. of garbage documents found in current cycle
     */
    void candidateDocuments(GCPhase mode, long numDocs);

    /**
     * Total No. of revisions detected as garbage during a given GC phase
     * @param mode GC phase
     * @param numRevs no. of garbage revisions found in current cycle
     */
    void candidateRevisions(GCPhase mode, long numRevs);

    /**
     * Total No. of internal revisions detected as garbage during a given GC phase
     * @param mode GC phase
     * @param numRevs no. of garbage internal revisions found in current cycle
     */
    void candidateInternalRevisions(GCPhase mode, long numRevs);

    /**
     * No. of orphan nodes deleted during FullGC
     * @param numNodes no. of orphan nodes deleted in current cycle
     */
    void orphanNodesDeleted(long numNodes);

    /**
     * No. of properties deleted during FullGC
     * @param numProps no. of properties deleted in current cycle
     */
    void propertiesDeleted(long numProps);

    /**
     * No. of unmerged (unique) branch commits deleted during FullGC
     * @param numCommits no. of unmerged branch commits deleted in current cycle
     */
    void unmergedBranchCommitsDeleted(long numCommits);

    /**
     * No. of documents updated (i.e. have garbage removed) during FullGC
     * @param numDocs no. of documents updated in current cycle
     */
    void documentsUpdated(long numDocs);

    /**
     * No. of documents which had skipped update (i.e. have been updated between garbage collection & removal)
     * during FullGC
     * @param numDocs No. of documents which had skipped update in current cycle
     */
    void documentsUpdateSkipped(long numDocs);

    /**
     * Total No. of documents that were skipped because of empty Split Props
     */
    void documentSkippedDueToEmptySplitProp();

    /**
     * No. of times the FullGC has started
     */
    void started();

    /**
     * Timer for different phases in FullGC
     * @param stats {@link VersionGCStats} containing FullGC phases timer
     */
    void finished(VersionGCStats stats);

    // FullGC OSGi config stats
    /**
     * Sets whether the FullGC process is enabled.
     * <p>
     * This method is called to signal whether the FullGC process is active and ready to perform garbage collection.
     *
     * @param enabled true if FullGC is enabled, false otherwise
     */
    void enabled(boolean enabled);

    /**
     * Sets the mode for the FullGC process.
     * <p>
     * This method is called to specify the mode in which the FullGC process should operate.
     *
     * @param mode the mode to set for the FullGC process
     */
    void mode(int mode);

    /**
     * Sets whether the embedded verification process is enabled for FullGC.
     * <p>
     * This method is called to signal whether the verification process is active and ready to perform embedded verification
     * during the FullGC process.
     *
     * @param verificationEnabled true if verification is enabled, false otherwise
     */
    void verificationEnabled(boolean verificationEnabled);

    /**
     * Sets the delay factor for the FullGC process.
     * <p>
     * This method is called to specify the delay factor that should be used during the FullGC process.
     *
     * @param delayFactor the delay factor to set for the FullGC process
     */
    void delayFactor(double delayFactor);

    /**
     * Sets the batch size for the FullGC process.
     * <p>
     * This method is called to specify the batch size that should be used during the FullGC process.
     *
     * @param batchSize the batch size to set for the FullGC process
     */
    void batchSize(int batchSize);

    /**
     * Sets the progress size for the FullGC process.
     * <p>
     * This method is called to specify the progress size that should be used during the FullGC process.
     *
     * @param progressSize the progress size to set for the FullGC process
     */
    void progressSize(int progressSize);

    /**
     * Sets the maximum age for the FullGC process (in millis).
     * <p>
     * This method is called to specify the maximum age that should be used during the FullGC process.
     *
     * @param maxAge the maximum age to set for the FullGC process
     */
    void maxAge(long maxAge);

    /**
     * Sets the full garbage collection generation value.
     * <p>
     * This method is called to update the current full GC generation being tracked.
     * The generation value is used to reset the full GC process when incremented,
     * allowing it to run from the beginning with fresh state.
     *
     * @param generation the full GC generation value to set
     */
    void fullGCGeneration(long generation);
}
