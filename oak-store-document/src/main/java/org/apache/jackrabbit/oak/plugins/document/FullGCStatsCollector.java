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
     * No. of times the FullGC has started
     */
    void started();

    /**
     * Timer for different phases in FullGC
     * @param stats {@link VersionGCStats} containing FullGC phases timer
     */
    void finished(VersionGCStats stats);
}
