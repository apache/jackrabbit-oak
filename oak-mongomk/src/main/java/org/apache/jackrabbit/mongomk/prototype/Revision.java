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
package org.apache.jackrabbit.mongomk.prototype;

/**
 * A revision.
 */
public class Revision {

    static long timestampOffset = java.sql.Timestamp.valueOf("2013-01-01 00:00:00.0").getTime() / 100;
    static volatile long lastTimestamp;
    static volatile int count;
    
    /**
     * The timestamp in milliseconds since 2013 (unlike in seconds since 1970 as
     * in MongoDB).
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
     * Compare the time part of two revisions.
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
        long timestamp = System.currentTimeMillis() / 100 - timestampOffset;
        int c;
        synchronized (Revision.class) {
            if (timestamp > lastTimestamp) {
                lastTimestamp = timestamp;
                c = count = 0;
            } else if (timestamp < lastTimestamp) {
                timestamp = lastTimestamp;
                c = ++count;
            } else {
                c = ++count;
            }
            if (c >= 0xfff) {
                timestamp++;
                c = 0;
                lastTimestamp = Math.max(timestamp, lastTimestamp);
            }
        }
        return new Revision(timestamp, c, clusterId);
    }
    
    public static Revision fromString(String rev) {
        if (!rev.startsWith("r")) {
            throw new IllegalArgumentException(rev);
        }
        String t = rev.substring(1, 8);
        long timestamp = Long.parseLong(t, 16);
        int idx = rev.indexOf('-');
        int c = 0;
        if (idx > 8) {
            t = rev.substring(9, 11);
            c = Integer.parseInt(t, 16);
        }
        t = rev.substring(idx + 1);
        int clusterId = Integer.parseInt(t, 16);
        Revision r = new Revision(timestamp, c, clusterId);
        return r;
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder("r");
        buff.append(Long.toHexString(0x10000000L + timestamp).substring(1));
        buff.append(Integer.toHexString(0x1000 + counter).substring(1));
        buff.append('-');
        buff.append(Integer.toHexString(clusterId));
        return buff.toString();
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
    
}
