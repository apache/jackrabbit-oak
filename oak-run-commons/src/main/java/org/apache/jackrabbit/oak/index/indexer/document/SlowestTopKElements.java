package org.apache.jackrabbit.oak.index.indexer.document;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class SlowestTopKElements {
    public static class Entry implements Comparable<Entry> {
        final String path;
        final long timeMillis;

        public Entry(String key, long timeMillis) {
            this.path = key;
            this.timeMillis = timeMillis;
        }

        @Override
        public int compareTo(Entry o) {
            return Long.compare(timeMillis, o.timeMillis);
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "path='" + path + '\'' +
                    ", timeMillis=" + timeMillis +
                    '}';
        }
    }

    private final int k;
    private final PriorityQueue<Entry> topSlowestPaths;

    public SlowestTopKElements(int k) {
        this.k = k;
        this.topSlowestPaths = new PriorityQueue<>(k);
    }

    public void add(String path, long timeMillis) {
        if (topSlowestPaths.size() < k) {
            topSlowestPaths.add(new Entry(path, timeMillis));
        } else if (topSlowestPaths.peek().timeMillis < timeMillis) {
            topSlowestPaths.poll();
            topSlowestPaths.add(new Entry(path, timeMillis));
        }
    }

    /**
     * Returns the top K slowest paths in descending order of time taken.
     */
    public Entry[] getTopK() {
        // PriorityQueue.toArray() does not guarantee any particular order, so we must sort the array
        Entry[] entries = topSlowestPaths.toArray(new Entry[0]);
        Arrays.sort(entries, Comparator.comparing(e -> -e.timeMillis));
        return entries;
    }

    @Override
    public String toString() {
        return Arrays.stream(getTopK())
                .map(e -> e.path + ": " + e.timeMillis)
                .collect(Collectors.joining("; ", "[", "]"));
    }
}