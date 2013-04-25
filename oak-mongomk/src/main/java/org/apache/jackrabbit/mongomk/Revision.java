/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A revision.
 */
public class Revision {

    private static volatile long lastTimestamp;

    private static volatile long lastRevisionTimestamp;
    private static volatile int lastRevisionCount;
    
    /**
     * The timestamp in milliseconds since 1970 (unlike in seconds as in
     * MongoDB). The timestamp is local to the machine that generated the
     * revision, such that timestamps of revisions can only be compared if the
     * machine id is the same.
     */
    private long timestamp;
    
    /**
     * An incrementing counter, for commits that occur within the same
     * millisecond.
     */
    private int counter;
    
    /**
     * The cluster id (the MongoDB machine id).
     */
    private int clusterId;
    
    public Revision(long timestamp, int counter, int clusterId) {
        this.timestamp = timestamp;
        this.counter = counter;
        this.clusterId = clusterId;
    }
    
    /**
     * Compare the time part of two revisions. If they contain the same time,
     * the counter is compared.
     * 
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     */
    int compareRevisionTime(Revision other) {
        int comp = timestamp < other.timestamp ? -1 : timestamp > other.timestamp ? 1 : 0;
        if (comp == 0) {
            comp = counter < other.counter ? -1 : counter > other.counter ? 1 : 0;
        }
        return comp;
    }
    
    /**
     * Create a simple revision id. The format is similar to MongoDB ObjectId.
     * 
     * @param clusterId the unique machineId + processId
     * @return the unique revision id
     */
    static Revision newRevision(int clusterId) {
        long timestamp = getCurrentTimestamp();
        int c;
        synchronized (Revision.class) {
            if (timestamp == lastRevisionTimestamp) {
                c = ++lastRevisionCount;
            } else {
                lastRevisionTimestamp = timestamp;
                lastRevisionCount = c = 0;
            }
        }
        return new Revision(timestamp, c, clusterId);
    }
    
    /**
     * Get the timestamp value of the current date and time. Within the same
     * process, the returned value is never smaller than a previously returned
     * value, even if the system time was changed.
     * 
     * @return the timestamp
     */
    public static long getCurrentTimestamp() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            // protect against decreases in the system time,
            // time machines, and other fluctuations in the time continuum
            timestamp = lastTimestamp;
        } else if (timestamp > lastTimestamp) {
            lastTimestamp = timestamp;
        }
        return timestamp;
    }
    
    /**
     * Get the difference between two timestamps (a - b) in milliseconds.
     * 
     * @param a the first timestamp
     * @param b the second timestamp
     * @return the difference in milliseconds
     */
    public static long getTimestampDifference(long a, long b) {
        return a - b;
    }
    
    public static Revision fromString(String rev) {
        if (!rev.startsWith("r")) {
            throw new IllegalArgumentException(rev);
        }
        int idxCount = rev.indexOf('-');
        if (idxCount < 0) {
            throw new IllegalArgumentException(rev);
        }
        int idxClusterId = rev.indexOf('-', idxCount + 1);
        if (idxClusterId < 0) {
            throw new IllegalArgumentException(rev);
        }
        String t = rev.substring(1, idxCount);
        long timestamp = Long.parseLong(t, 16);
        t = rev.substring(idxCount + 1, idxClusterId);
        int c = Integer.parseInt(t, 16);
        t = rev.substring(idxClusterId + 1);
        int clusterId = Integer.parseInt(t, 16);
        Revision r = new Revision(timestamp, c, clusterId);
        return r;
    }
    
    public String toString() {
        return new StringBuilder("r").
                append(Long.toHexString(timestamp)).
                append('-').
                append(Integer.toHexString(counter)).
                append('-').
                append(Integer.toHexString(clusterId)).
                toString();
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    public int getCounter() {
        return counter;
    }
    
    public int hashCode() {
        return (int) (timestamp >>> 32) ^ (int) timestamp ^ counter ^ clusterId;
    }
    
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other.getClass() != this.getClass()) {
            return false;
        }
        Revision r = (Revision) other;
        return r.timestamp == this.timestamp && 
                r.counter == this.counter && 
                r.clusterId == this.clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }
    
    /**
     * Revision ranges allow to compare revisions ids of different cluster instances. A
     * range tells when a list of revisions from a certain cluster instance was seen by
     * the current process.
     */
    static class RevisionRange {
        
        /**
         * The newest revision for the given cluster instance and time.
         */
        Revision revision;

        /**
         * The (local) timestamp; the time when this revision was seen by this
         * cluster instance.
         */
        long timestamp;
        
        public String toString() {
            return revision + ":" + timestamp;
        }
        
    }
    
    /**
     * A facility that is able to compare revisions of different cluster instances.
     * It contains a map of revision ranges.
     */
    public static class RevisionComparator implements Comparator<Revision> {
        
        /**
         * The map of cluster instances to lists of revision ranges.
         */
        private final ConcurrentMap<Integer, List<RevisionRange>> map = 
                new ConcurrentHashMap<Integer, List<RevisionRange>>();
        
        /**
         * When comparing revisions that occurred before, the timestamp is ignored.
         */
        private long oldestTimestamp;

        /**
         * Forget the order of older revisions. After calling this method, when comparing
         * revisions that happened before the given value, the timestamp order is used
         * (time dilation is ignored for older events).
         * 
         * @param timestamp the time in milliseconds (see {@link #getCurrentTimestamp})
         */
        public void purge(long timestamp) {
            oldestTimestamp = timestamp;
            for (int clusterId : map.keySet()) {
                while (true) {
                    List<RevisionRange> list = map.get(clusterId);
                    List<RevisionRange> newList = purge(list);
                    if (newList == null) {
                        // retry if removing was not successful
                        if (map.remove(clusterId, list)) {
                            break;
                        }
                    } else if (newList == list) {
                        // no change
                        break;
                    } else {
                        // retry if replacing was not successful
                        if (map.replace(clusterId, list, newList)) {
                            break;
                        }
                    }
                }
            } 
        }

        private List<RevisionRange> purge(List<RevisionRange> list) {
            int i = 0;
            for (; i < list.size(); i++) {
                RevisionRange r = list.get(i);
                if (r.timestamp > oldestTimestamp) {
                    break;
                }
            }
            if (i > list.size() - 1) {
                return null;
            } else if (i == 0) {
                return list;
            }
            return new ArrayList<RevisionRange>(list.subList(i, list.size()));
        }
        
        /**
         * Add the revision to the top of the queue for the given cluster node.
         * If an entry for this timestamp already exists, it is replaced.
         * 
         * @param r the revision
         * @param timestamp the timestamp
         */
        public void add(Revision r, long timestamp) {
            int clusterId = r.getClusterId();
            while (true) {
                List<RevisionRange> list = map.get(clusterId);
                List<RevisionRange> newList;
                if (list == null) {
                    newList = new ArrayList<RevisionRange>();
                } else {
                    RevisionRange last = list.get(list.size() - 1);
                    if (last.timestamp == timestamp) {
                        last.revision = r;
                        return;
                    }
                    newList = new ArrayList<RevisionRange>(list);
                }
                RevisionRange range = new RevisionRange();
                range.timestamp = timestamp;
                range.revision = r;
                newList.add(range);
                if (list == null) {
                    if (map.putIfAbsent(clusterId, newList) == null) {
                        return;
                    }                    
                } else {
                    if (map.replace(clusterId, list, newList)) {
                        return;
                    }
                }
            }
        }
        
        @Override
        public int compare(Revision o1, Revision o2) {
            if (o1.getClusterId() == o2.getClusterId()) {
                return o1.compareRevisionTime(o2);
            }
            RevisionRange range1 = getRevisionRange(o1);
            RevisionRange range2 = getRevisionRange(o2);
            if (range1 == null || range2 == null) {
                return o1.compareRevisionTime(o2);
            }
            if (range1.timestamp != range2.timestamp) {
                return range1.timestamp < range2.timestamp ? -1 : 1;
            }
            int result = o1.compareRevisionTime(o2);
            if (result != 0) {
                return result;
            }
            return o1.getClusterId() < o2.getClusterId() ? -1 : 1;
        }
        
        private RevisionRange getRevisionRange(Revision r) {
            List<RevisionRange> list = map.get(r.getClusterId());
            if (list == null) {
                return null;
            }
            // search from latest backward
            // (binary search could be used, but we expect most queries
            // at the end of the list)
            for (int i = list.size() - 1; i >= 0; i--) {
                RevisionRange range = list.get(i);
                if (r.compareRevisionTime(range.revision) >= 0) {
                    return range;
                }
            }
            return null;
        }
        
        public String toString() {
            StringBuilder buff = new StringBuilder();
            for (int clusterId : new TreeSet<Integer>(map.keySet())) {
                buff.append(clusterId).append(':');
                for (RevisionRange r : map.get(clusterId)) {
                    buff.append(' ').append(r);
                }
                buff.append("; ");
            }
            return buff.toString();
        }
        
    }

}
