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
package org.apache.jackrabbit.oak.index.indexer.document;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * Keeps track of the top K entries that took the longest to index.
 */
final class TopKSlowestPaths {
    final static class PathAndTime implements Comparable<PathAndTime> {
        final String path;
        final long timeMillis;

        public PathAndTime(String key, long timeMillis) {
            this.path = key;
            this.timeMillis = timeMillis;
        }

        @Override
        public int compareTo(PathAndTime o) {
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
    private final PriorityQueue<PathAndTime> topKSlowestPaths;

    public TopKSlowestPaths(int k) {
        this.k = k;
        this.topKSlowestPaths = new PriorityQueue<>(k);
    }

    public void add(String path, long timeMillis) {
        if (topKSlowestPaths.size() < k) {
            // Not full, add the entry
            topKSlowestPaths.add(new PathAndTime(path, timeMillis));
        } else if (topKSlowestPaths.peek().timeMillis < timeMillis) {
            // The new entry took longer to index than the current bottom slowest entry. Replace it.
            topKSlowestPaths.poll();
            topKSlowestPaths.add(new PathAndTime(path, timeMillis));
        }
    }

    /**
     * Returns the top K paths by descending order of the time associated with them.
     */
    public PathAndTime[] getTopK() {
        // PriorityQueue.toArray() does not guarantee any particular order, so we must sort the array
        PathAndTime[] entries = topKSlowestPaths.toArray(new PathAndTime[0]);
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