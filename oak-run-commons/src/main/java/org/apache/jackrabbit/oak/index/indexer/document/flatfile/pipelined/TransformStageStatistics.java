package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class TransformStageStatistics {
    private final LongAdder mongoDocumentsProcessed = new LongAdder();
    private final LongAdder entriesExtracted = new LongAdder();
    private final LongAdder extractedEntriesTotalSize = new LongAdder();
    private final LongAdder splitDocumentsRejected = new LongAdder();
    private final LongAdder emptyNodeStateDocuments = new LongAdder();
    private final LongAdder entriesRejected = new LongAdder();
    private final LongAdder entriesRejectedHiddenPaths = new LongAdder();
    private final LongAdder entriesRejectedPathFiltered = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> hiddenPathsRejectedHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> filteredPathsRejectedHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> splitDocumentsHistogram = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> emptyNodeStateHistogram = new ConcurrentHashMap<>();

    public long getMongoDocumentsProcessed() {
        return mongoDocumentsProcessed.sum();
    }

    public long getEntriesExtracted() {
        return entriesExtracted.sum();
    }

    public long getEntriesRejected() {
        return entriesRejected.sum();
    }

    public LongAdder getSplitDocumentsRejected() {
        return splitDocumentsRejected;
    }

    public ConcurrentHashMap<String, LongAdder> getHiddenPathsRejected() {
        return hiddenPathsRejectedHistogram;
    }

    public void incrementMongoDocumentsProcessed() {
        mongoDocumentsProcessed.increment();
    }

    public void incrementEntriesExtracted() {
        entriesExtracted.increment();
    }

    public void incrementSplitDocuments() {
        this.splitDocumentsRejected.increment();
    }

    public void addSplitDocument(String id) {
        this.splitDocumentsRejected.increment();
        this.splitDocumentsHistogram.computeIfAbsent(getPathPrefix(id, 5), k -> new LongAdder()).increment();
    }

    public void addEmptyNodeStateEntry(String nodeId) {
        this.emptyNodeStateDocuments.increment();
        this.emptyNodeStateHistogram.computeIfAbsent(getPathPrefix(nodeId, 5), k -> new LongAdder()).increment();
    }

    public void incrementTotalExtractedEntriesSize(int entrySize) {
        this.extractedEntriesTotalSize.add(entrySize);
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


//    public void incrementPathsFiltered() {
//        this.entriesRejectedPathFiltered.increment();
//    }


    @Override
    public String toString() {
        return "TransformStageStatistics{" +
                "mongoDocumentsProcessed=" + mongoDocumentsProcessed +
                ", entriesExtracted=" + entriesExtracted +
                ", extractedEntriesTotalSize=" + extractedEntriesTotalSize +
                ", splitDocumentsRejected=" + splitDocumentsRejected +
                ", emptyNodeStateDocuments=" + emptyNodeStateDocuments +
                ", entriesRejected=" + entriesRejected +
                ", entriesRejectedHiddenPaths=" + entriesRejectedHiddenPaths +
                ", entriesRejectedPathFiltered=" + entriesRejectedPathFiltered +
                '}';
    }

    public String formatStats() {
        String avgEntrySize = entriesExtracted.sum() == 0 ? "N/A" : Long.toString(extractedEntriesTotalSize.sum() / entriesExtracted.sum());
        String ratioExtractedEntries = mongoDocumentsProcessed.sum() == 0 ? "N/A" : String.format("%1.2f", (1.0 * entriesExtracted.sum()) / mongoDocumentsProcessed.sum());
        long totalEntries = entriesExtracted.sum() + entriesRejected.sum();
        String entriesRejectedFractionStr = totalEntries == 0 ? "N/A" : String.format("%1.2f", (1.0 * entriesRejected.sum()) / totalEntries);
        return "mongoDocumentsProcessed=" + mongoDocumentsProcessed.sum() +
                ", splitDocumentsRejected=" + splitDocumentsRejected +
                ", emptyNodeStateDocuments=" + emptyNodeStateDocuments +
                ", entriesExtracted=" + entriesExtracted.sum() +
                ", entriesExtracted/mongoDocuments=" + ratioExtractedEntries +
                ", entriesRejected=" + entriesRejected.sum() +
                ", entriesRejectedHiddenPaths=" + entriesRejectedHiddenPaths +
                ", entriesRejectedPathFiltered=" + entriesRejectedPathFiltered +
                ", entriesRejectedFraction=" + entriesRejectedFractionStr +
                ", extractedEntriesTotalSize=" + FileUtils.byteCountToDisplaySize(extractedEntriesTotalSize.sum()) +
                ", avgEntrySize=" + avgEntrySize;
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
                .collect(Collectors.joining(", ", "[", "]"));
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
