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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class TransformStageStatistics {
    private final LongAdder mongoDocumentsTraversed = new LongAdder();
    private final LongAdder documentsRejectedSplit = new LongAdder();
    private final LongAdder documentsRejectedEmptyNodeState = new LongAdder();
    private final LongAdder entriesAccepted = new LongAdder();
    private final LongAdder entriesRejected = new LongAdder();
    private final LongAdder entriesRejectedHiddenPaths = new LongAdder();
    private final LongAdder entriesRejectedPathFiltered = new LongAdder();
    private final LongAdder entriesAcceptedTotalSize = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> hiddenPathsRejectedHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> filteredPathsRejectedHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> splitDocumentsHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> emptyNodeStateHistogram = new ConcurrentHashMap<>();

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

    public ConcurrentHashMap<String, LongAdder> getHiddenPathsRejected() {
        return hiddenPathsRejectedHistogram;
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

    public void addSplitDocument(String id) {
        this.documentsRejectedSplit.increment();
        this.splitDocumentsHistogram.computeIfAbsent(getPathPrefix(id, 5), k -> new LongAdder()).increment();
    }

    public void addEmptyNodeStateEntry(String nodeId) {
        this.documentsRejectedEmptyNodeState.increment();
        // TODO: bound the number of entries in the histogram
        this.emptyNodeStateHistogram.computeIfAbsent(getPathPrefix(nodeId, 5), k -> new LongAdder()).increment();
    }

    public void incrementTotalExtractedEntriesSize(int entrySize) {
        this.entriesAcceptedTotalSize.add(entrySize);
    }

    public void incrementEntriesRejected() {
        this.entriesRejected.increment();
    }

    public void addRejectedHiddenPath(String path) {
        this.entriesRejectedHiddenPaths.increment();
        this.hiddenPathsRejectedHistogram.computeIfAbsent(getPathPrefix(path, 3), k -> new LongAdder()).increment();
    }

    public void addRejectedFilteredPath(String path) {
        this.entriesRejectedPathFiltered.increment();
        this.filteredPathsRejectedHistogram.computeIfAbsent(getPathPrefix(path, 3), k -> new LongAdder()).increment();
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

    public String prettyPrintHiddenPathsHistogram() {
        return prettyPrintMap(hiddenPathsRejectedHistogram);
    }

    public String prettyPrintPathFilteredHistogram() {
        return prettyPrintMap(filteredPathsRejectedHistogram);
    }

    public String prettyPrintSplitDocumentsHistogram() {
        return prettyPrintMap(splitDocumentsHistogram);
    }

    public String prettyPrintEmptyNodeStateHistogram() {
        return prettyPrintMap(emptyNodeStateHistogram);
    }

    private static String prettyPrintMap(ConcurrentHashMap<String, LongAdder> map) {
        return map.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().sum()))
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue())) // sort by value descending
                .limit(20)
                .map(e -> "\"" + e.getKey() + "\":" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
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
