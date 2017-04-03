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

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A revision.
 */
public final class Revision implements CacheValue {

    //Extra 2 for those cases where counter or clusterId is 2 digit
    final static int REV_STRING_APPROX_SIZE = Revision.newRevision(0).toString().length() + 2;

    static final int SHALLOW_MEMORY_USAGE = 32;

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
        checkNotNull(c);
        clock = c;
        lastTimestamp = clock.getTime();
        lastRevisionTimestamp = clock.getTime();
    }

    /**
     * <b>
     * Only to be used for testing.
     * Do Not Use Otherwise
     * </b>
     */
    static void resetClockToDefault() {
        setClock(Clock.SIMPLE);
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
     * Compare all components of two revisions.
     * 
     * @param other the other revision
     * @return -1, 0, or 1
     */
    int compareTo(Revision other) {
        int comp = compareRevisionTimeThenClusterId(other);
        if (comp == 0) {
            if (branch != other.branch) {
                return branch ? -1 : 1;
            }
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
            // need to check again, because threads
            // could arrive inside the synchronized block
            // out of order
            if (timestamp < lastRevisionTimestamp) {
                timestamp = lastRevisionTimestamp;
            }
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
        boolean isBranch = rev.charAt(0) == 'b';
        int idx = isBranch ? 2 : 1;
        if (rev.charAt(idx - 1) != 'r') {
            throw new IllegalArgumentException(rev);
        }
        int len = rev.length();
        // Parse timestamp
        long timestamp = 0;
        for (; idx < len; idx++) {
            char c = rev.charAt(idx);
            if (c == '-') {
                break;
            }
            int digit = c >= 'a'? c - 'a' + 10 : c - '0';
            timestamp = (timestamp << 4) + digit;
        }
        // Parse counter
        int counter = 0;
        for (idx++; idx < len; idx++) {
            char c = rev.charAt(idx);
            if (c == '-') {
                break;
            }
            int digit = c >= 'a' ? c - 'a' + 10 : c - '0';
            counter = (counter << 4) + digit;
        }
        // Parse clusterId
        int clusterId = 0;
        for (idx++; idx < len; idx++) {
            char c = rev.charAt(idx);
            int digit = c >= 'a' ? c - 'a' + 10 : c - '0';
            clusterId = (clusterId << 4) + digit;
        }
        return new Revision(timestamp, counter, clusterId, isBranch);
    }

    @Override
    public String toString() {
        return toStringBuilder(new StringBuilder(REV_STRING_APPROX_SIZE)).toString();
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
        toHexString(sb, timestamp);
        sb.append('-');
        toHexString(sb, counter);
        sb.append('-');
        toHexString(sb, clusterId);
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
     * @return trunk revision with this timestamp, counter and clusterId.
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

    public int getClusterId() {
        return clusterId;
    }

    @Override
    public int getMemory() {
        return SHALLOW_MEMORY_USAGE;
    }

    private static void toHexString(StringBuilder sb, long x) {
        int bitCount = (64 - Long.numberOfLeadingZeros(x));
        bitCount = Math.max(0,  ((bitCount + 3) / 4 * 4) - 4);
        for (int i = bitCount; i >= 0; i -= 4) {
            int t = (int) (x >> i) & 15;
            if (t > 9) {
                t = t - 10 + 'a';
            } else {
                t += '0';
            }
            sb.append((char) t);
        }
    }
    
}
