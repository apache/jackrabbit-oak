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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
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
    private final long timestamp;

    /**
     * An incrementing counter, for commits that occur within the same
     * millisecond.
     */
    private final int counter;

    /**
     * The cluster id (the MongoDB machine id).
     */
    private final int clusterId;

    /**
     * Whether this is a branch revision.
     */
    private final boolean branch;

    /** Only set for testing */
    private static Clock clock;

    /**
     * <b>
     * Only to be used for testing.
     * Do Not Use Otherwise
     * </b>
     * 
     * @param c - the clock
     */
    static void setClock(Clock c) {
        clock = c;
        if (c == null) {
            lastTimestamp = System.currentTimeMillis();
        }
    }
    public Revision(long timestamp, int counter, int clusterId) {
        this(timestamp, counter, clusterId, false);
    }

    public Revision(long timestamp, int counter, int clusterId, boolean branch) {
        this.timestamp = timestamp;
        this.counter = counter;
        this.clusterId = clusterId;
        this.branch = branch;
    }

    /**
     * Compare the time part of two revisions. If they contain the same time,
     * the counter is compared.
     * <p>
     * This method requires that both revisions are from the same cluster node.
     *
     * @param other the other revision
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     * @throws IllegalArgumentException if the cluster ids don't match
     */
    int compareRevisionTime(Revision other) {
        if (clusterId != other.clusterId) {
            throw new IllegalArgumentException(
                    "Trying to compare revisions of different cluster ids: " +
                            this + " and " + other);
        }
        int comp = timestamp < other.timestamp ? -1 : timestamp > other.timestamp ? 1 : 0;
        if (comp == 0) {
            comp = counter < other.counter ? -1 : counter > other.counter ? 1 : 0;
        }
        return comp;
    }

    /**
     * Compare the time part of two revisions. If they contain the same time,
     * the counter is compared. If the counter is the same, the cluster ids are
     * compared.
     *
     * @param other the other revision
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     */
    int compareRevisionTimeThenClusterId(Revision other) {
        int comp = timestamp < other.timestamp ? -1 : timestamp > other.timestamp ? 1 : 0;
        if (comp == 0) {
            comp = counter < other.counter ? -1 : counter > other.counter ? 1 : 0;
        }
        if (comp == 0) {
            comp = compareClusterId(other);
        }
        return comp;
    }

    /**
     * Compare the cluster node ids of both revisions.
     *
     * @param other the other revision
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     */
    int compareClusterId(Revision other) {
        return clusterId < other.clusterId ? -1 : clusterId > other.clusterId ? 1 : 0;
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
        if (clock != null) {
            timestamp = clock.getTime();
        }
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
     * Get the timestamp difference between two revisions (r1 - r2) in
     * milliseconds.
     *
     * @param r1 the first revision
     * @param r2 the second revision
     * @return the difference in milliseconds
     */
    public static long getTimestampDifference(Revision r1, Revision r2) {
        return r1.getTimestamp() - r2.getTimestamp();
    }

    public static Revision fromString(String rev) {
        boolean isBranch = false;
        if (rev.startsWith("b")) {
            isBranch = true;
            rev = rev.substring(1);
        }
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
        return new Revision(timestamp, c, clusterId, isBranch);
    }

    @Override
    public String toString() {
        return toStringBuilder(new StringBuilder()).toString();
    }

    /**
     * Appends the string representation of this revision to the given
     * StringBuilder.
     *
     * @param sb a StringBuilder.
     * @return the StringBuilder instance passed to this method.
     */
    public StringBuilder toStringBuilder(StringBuilder sb) {
        if (branch) {
            sb.append('b');
        }
        sb.append('r');
        sb.append(Long.toHexString(timestamp)).append('-');
        if (counter < 10) {
            sb.append(counter);
        } else {
            sb.append(Integer.toHexString(counter));
        }
        sb.append('-');
        if (clusterId < 10) {
            sb.append(clusterId);
        } else {
            sb.append(Integer.toHexString(clusterId));
        }
        return sb;
    }

    public String toReadableString() {
        StringBuilder buff = new StringBuilder();
        buff.append("revision: \"").append(toString()).append("\"");
        buff.append(", clusterId: ").append(clusterId);
        buff.append(", time: \"").
            append(Utils.timestampToString(timestamp)).
            append("\"");
        if (counter > 0) {
            buff.append(", counter: ").append(counter);
        }
        if (branch) {
            buff.append(", branch: true");
        }
        return buff.toString();
    }

    /**
     * Get the timestamp in milliseconds since 1970.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    public int getCounter() {
        return counter;
    }

    /**
     * @return <code>true</code> if this is a branch revision, otherwise
     *         <code>false</code>.
     */
    public boolean isBranch() {
        return branch;
    }

    /**
     * Returns a revision with the same timestamp, counter and clusterId as this
     * revision and the branch flag set to <code>true</code>.
     *
     * @return branch revision with this timestamp, counter and clusterId.
     */
    public Revision asBranchRevision() {
        if (isBranch()) {
            return this;
        } else {
            return new Revision(timestamp, counter, clusterId, true);
        }
    }

    /**
     * Returns a revision with the same timestamp, counter and clusterId as this
     * revision and the branch flag set to <code>false</code>.
     *
     * @return trunkrevision with this timestamp, counter and clusterId.
     */
    public Revision asTrunkRevision() {
        if (!isBranch()) {
            return this;
        } else {
            return new Revision(timestamp, counter, clusterId);
        }
    }

    @Override
    public int hashCode() {
        return (int) (timestamp >>> 32) ^ (int) timestamp ^ counter ^ clusterId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else if (other.getClass() != this.getClass()) {
            return false;
        }
        Revision r = (Revision) other;
        return r.timestamp == this.timestamp &&
                r.counter == this.counter &&
                r.clusterId == this.clusterId &&
                r.branch == this.branch;
    }

    public boolean equalsIgnoreBranch(Revision other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        }
        return other.timestamp == this.timestamp &&
                other.counter == this.counter &&
                other.clusterId == this.clusterId;
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
         * The (local) revision; the time when this revision was seen by this
         * cluster instance.
         */
        Revision seenAt;

        @Override
        public String toString() {
            return revision + ":" + seenAt;
        }

    }

    /**
     * A facility that is able to compare revisions of different cluster instances.
     * It contains a map of revision ranges.
     */
    public static class RevisionComparator implements Comparator<Revision> {

        static final Revision NEWEST = new Revision(Long.MAX_VALUE, 0, 0);

        static final Revision FUTURE = new Revision(Long.MAX_VALUE, Integer.MAX_VALUE, 0);

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
         * The cluster node id of the current cluster node. Revisions
         * from this cluster node that are newer than the newest range
         * (new local revisions)
         * are considered to be the newest revisions overall.
         */
        private final int currentClusterNodeId;

        RevisionComparator(int currentClusterNodId) {
            this.currentClusterNodeId = currentClusterNodId;
        }

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
                if (r.seenAt.getTimestamp() > oldestTimestamp) {
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
         * @param seenAt the (local) revision where this revision was seen here
         */
        public void add(Revision r, Revision seenAt) {
            int clusterId = r.getClusterId();
            while (true) {
                List<RevisionRange> list = map.get(clusterId);
                List<RevisionRange> newList;
                if (list == null) {
                    newList = new ArrayList<RevisionRange>();
                } else {
                    RevisionRange last = list.get(list.size() - 1);
                    if (last.seenAt.equals(seenAt)) {
                        // replace existing
                        if (r.compareRevisionTime(last.revision) > 0) {
                            // but only if newer
                            last.revision = r;
                        }
                        return;
                    }
                    if (last.revision.compareRevisionTime(r) > 0) {
                        throw new IllegalArgumentException(
                                "Can not add an earlier revision: " + last.revision + " > " + r +
                                "; current cluster node is " + currentClusterNodeId);
                    }
                    newList = new ArrayList<RevisionRange>(list);
                }
                RevisionRange range = new RevisionRange();
                range.seenAt = seenAt;
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
            Revision range1 = getRevisionSeen(o1);
            Revision range2 = getRevisionSeen(o2);
            if (range1 == FUTURE && range2 == FUTURE) {
                return o1.compareRevisionTimeThenClusterId(o2);
            }
            if (range1 == null || range2 == null) {
                return o1.compareRevisionTimeThenClusterId(o2);
            }
            int comp = range1.compareRevisionTimeThenClusterId(range2);
            if (comp != 0) {
                return comp;
            }
            return Integer.signum(o1.getClusterId() - o2.getClusterId());
        }

        /**
         * Get the seen-at revision from the revision range.
         * <p>
         * <ul>
         *     <li>
         *         {@code null} if the revision is older than the earliest range
         *     </li>
         *     <li>
         *         if the revision is newer than the lower bound of the newest
         *         range, then {@link #NEWEST} is returned for a local cluster
         *         revision and {@link #FUTURE} for a foreign cluster revision.
         *     </li>
         *     <li>
         *         if the revision matches the lower seen-at bound of a range,
         *         then this seen-at revision is returned.
         *     </li>
         *     <li>
         *         otherwise the lower bound seen-at revision of next higher
         *         range is returned.
         *     </li>
         * </ul>
         *
         * @param r the revision
         * @return the seen-at revision or {@code null} if the revision is older
         *          than the earliest range.
         */
        Revision getRevisionSeen(Revision r) {
            List<RevisionRange> list = map.get(r.getClusterId());
            if (list == null) {
                if (r.getClusterId() != currentClusterNodeId) {
                    // this is from a cluster node we did not see yet
                    // see also OAK-1170
                    return FUTURE;
                }
                return null;
            }
            // search from latest backward
            // (binary search could be used, but we expect most queries
            // at the end of the list)
            for (int i = list.size() - 1; i >= 0; i--) {
                RevisionRange range = list.get(i);
                int compare = r.compareRevisionTime(range.revision);
                if (compare == 0) {
                    return range.seenAt;
                } else if (compare > 0) {
                    if (i == list.size() - 1) {
                        // newer than the newest range
                        if (r.getClusterId() == currentClusterNodeId) {
                            // newer than all others, except for FUTURE
                            return NEWEST;
                        }
                        // happens in the future (not visible yet)
                        return FUTURE;
                    } else {
                        // there is a newer range
                        return list.get(i + 1).seenAt;
                    }
                }
            }
            return null;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            for (int clusterId : new TreeSet<Integer>(map.keySet())) {
                int i = 0;
                buff.append(clusterId).append(":");
                for (RevisionRange r : map.get(clusterId)) {
                    if (i++ % 4 == 0) {
                        buff.append('\n');
                    }
                    buff.append(" ").append(r);
                }
                buff.append("\n");
            }
            return buff.toString();
        }

    }

}
