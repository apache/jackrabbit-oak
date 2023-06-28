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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.LongAdder;

public class TransformStageStatistics {
    private static final int MAX_HISTOGRAM_SIZE = 1000;
    private final LongAdder mongoDocumentsTraversed = new LongAdder();
    private final LongAdder documentsRejectedSplit = new LongAdder();
    private final LongAdder documentsRejectedEmptyNodeState = new LongAdder();
    private final LongAdder entriesAccepted = new LongAdder();
    private final LongAdder entriesRejected = new LongAdder();
    private final LongAdder entriesRejectedHiddenPaths = new LongAdder();
    private final LongAdder entriesRejectedPathFiltered = new LongAdder();
    private final LongAdder entriesAcceptedTotalSize = new LongAdder();
    private final BoundedHistogram hiddenPathsRejectedHistogram = new BoundedHistogram("Hidden paths", MAX_HISTOGRAM_SIZE);
    private final BoundedHistogram filteredPathsRejectedHistogram = new BoundedHistogram("Filtered paths", MAX_HISTOGRAM_SIZE);
    private final BoundedHistogram splitDocumentsHistogram = new BoundedHistogram("Split documents", MAX_HISTOGRAM_SIZE);
    private final BoundedHistogram emptyNodeStateHistogram = new BoundedHistogram("Empty node state", MAX_HISTOGRAM_SIZE);

    public long getMongoDocumentsTraversed() {
        return mongoDocumentsTraversed.sum();
    }

    public long getEntriesAccepted() {
        return entriesAccepted.sum();
    }

    public long getEntriesRejected() {
        return entriesRejected.sum();
    }

    public LongAdder getDocumentsRejectedSplit() {
        return documentsRejectedSplit;
    }

    public BoundedHistogram getHiddenPathsRejectedHistogram() {
        return hiddenPathsRejectedHistogram;
    }

    public BoundedHistogram getFilteredPathsRejectedHistogram() {
        return filteredPathsRejectedHistogram;
    }

    public BoundedHistogram getSplitDocumentsHistogram() {
        return splitDocumentsHistogram;
    }

    public BoundedHistogram getEmptyNodeStateHistogram() {
        return emptyNodeStateHistogram;
    }

    public void incrementMongoDocumentsProcessed() {
        mongoDocumentsTraversed.increment();
    }

    public void incrementEntriesAccepted() {
        entriesAccepted.increment();
    }

    public void incrementSplitDocuments() {
        this.documentsRejectedSplit.increment();
    }

    public void incrementTotalExtractedEntriesSize(int entrySize) {
        this.entriesAcceptedTotalSize.add(entrySize);
    }

    public void incrementEntriesRejected() {
        this.entriesRejected.increment();
    }

    public void addSplitDocument(String id) {
        this.documentsRejectedSplit.increment();
        String key = getPathPrefix(id, 5);
        this.splitDocumentsHistogram.addEntry(key);
    }

    public void addEmptyNodeStateEntry(String nodeId) {
        this.documentsRejectedEmptyNodeState.increment();
        String key = getPathPrefix(nodeId, 5);
        this.emptyNodeStateHistogram.addEntry(key);
    }

    public void addRejectedHiddenPath(String path) {
        this.entriesRejectedHiddenPaths.increment();
        String key = getPathPrefix(path, 3);
        this.hiddenPathsRejectedHistogram.addEntry(key);
    }

    public void addRejectedFilteredPath(String path) {
        this.entriesRejectedPathFiltered.increment();
        String key = getPathPrefix(path, 3);
        this.filteredPathsRejectedHistogram.addEntry(key);
    }

    @Override
    public String toString() {
        return "TransformStageStatistics{" +
                "mongoDocumentsProcessed=" + mongoDocumentsTraversed +
                ", splitDocumentsRejected=" + documentsRejectedSplit +
                ", emptyNodeStateDocuments=" + documentsRejectedEmptyNodeState +
                ", entriesAccepted=" + entriesAccepted +
                ", entriesRejected=" + entriesRejected +
                ", entriesRejectedHiddenPaths=" + entriesRejectedHiddenPaths +
                ", entriesRejectedPathFiltered=" + entriesRejectedPathFiltered +
                ", extractedEntriesTotalSize=" + entriesAcceptedTotalSize +
                '}';
    }

    public String formatStats() {
        long mongoDocumentsTraversedSum = mongoDocumentsTraversed.sum();
        long entriesAcceptedSum = entriesAccepted.sum();
        long extractedEntriesTotalSizeSum = entriesAcceptedTotalSize.sum();
        long entriesRejectedSum = entriesRejected.sum();
        long documentsRejectedSplitSum = documentsRejectedSplit.sum();
        long documentsRejectedEmptyNodeStateSum = documentsRejectedEmptyNodeState.sum();
        long documentsRejectedTotal = documentsRejectedSplitSum + documentsRejectedEmptyNodeStateSum;
        long documentsAcceptedTotal = mongoDocumentsTraversedSum - documentsRejectedTotal;
        long totalEntries = entriesAcceptedSum + entriesRejectedSum;
        String documentsAcceptedPercentage = mongoDocumentsTraversedSum == 0 ? "N/A" :
                String.format("%2.1f%%", (100.0 * documentsAcceptedTotal) / mongoDocumentsTraversedSum);
        String entriesAcceptedPercentage = totalEntries == 0 ? "N/A" :
                String.format("%1.1f%%", (100.0 * entriesAcceptedSum) / totalEntries);
        String avgEntrySize = entriesAcceptedSum == 0 ? "N/A" :
                Long.toString(extractedEntriesTotalSizeSum / entriesAcceptedSum);
        return "{documentsTraversed:" + mongoDocumentsTraversedSum +
                ", documentsAccepted:" + documentsAcceptedTotal +
                ", documentsRejected:" + documentsRejectedTotal +
                ", documentsAcceptedPercentage:" + documentsAcceptedPercentage +
                ", documentsRejectedSplit:" + documentsRejectedSplitSum +
                ", documentsRejectedEmptyNodeState:" + documentsRejectedEmptyNodeStateSum +
                ", entriesTraversed:" + totalEntries +
                ", entriesAccepted:" + entriesAcceptedSum +
                ", entriesRejected:" + entriesRejectedSum +
                ", entriesAcceptedPercentage:" + entriesAcceptedPercentage +
                ", entriesRejectedHiddenPaths:" + entriesRejectedHiddenPaths +
                ", entriesRejectedPathFiltered:" + entriesRejectedPathFiltered +
                ", extractedEntriesTotalSize:" + FileUtils.byteCountToDisplaySize(extractedEntriesTotalSizeSum) +
                ", avgEntrySize:" + avgEntrySize +
                "}";
    }

    private static String getPathPrefix(String path, int depth) {
        int idx = StringUtils.ordinalIndexOf(path, "/", depth + 1);
        if (idx == -1) {
            return path;
        } else {
            return path.substring(0, idx);
        }
    }
}
