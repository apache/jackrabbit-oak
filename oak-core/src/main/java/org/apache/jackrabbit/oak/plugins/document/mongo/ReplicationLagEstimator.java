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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.ReadPreference;

public class ReplicationLagEstimator implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLagEstimator.class);

    /**
     * How many replication lags should be taken into consideration for
     * computing the average.
     */
    private static final int STAT_SIZE = 10;

    private final Map<String, Stat> memberLags = new HashMap<String, Stat>();

    private final DB adminDb;

    private final long maxReplicationLagMillis;

    private final long pullFrequencyMillis;

    private volatile boolean stop;

    private volatile long lastEstimatedValue;

    private boolean unknownState;

    public ReplicationLagEstimator(DB adminDb, long maxReplicationLagMillis, long pullFrequencyMillis) {
        this.adminDb = adminDb;
        this.maxReplicationLagMillis = maxReplicationLagMillis;
        this.pullFrequencyMillis = pullFrequencyMillis;
    }

    public synchronized void stop() {
        stop = true;
        notify();
    }

    public long getEstimation() {
        return lastEstimatedValue;
    }

    @Override
    public void run() {
        while (!stop) {
            updateStats();
            lastEstimatedValue = estimate();
            synchronized (this) {
                try {
                    if (!stop) {
                        wait(pullFrequencyMillis);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private long estimate() {
        if (unknownState || memberLags.isEmpty()) {
            return maxReplicationLagMillis;
        } else {
            LOG.debug("Average lags for the instances: {}", memberLags);
            Long max = null;
            for (Stat s : memberLags.values()) {
                Long avg = s.average();
                if (max == null || max < avg) {
                    max = avg;
                }
            }
            LOG.debug("Estimated maximum lag is: {}", max);
            return max;
        }
    }

    /**
     * Read the replication lag for each member by comparing PRIMARY and
     * SECONDARY nodes last optime value.
     */
    @SuppressWarnings("unchecked")
    private void updateStats() {
        CommandResult result = adminDb.command("replSetGetStatus", ReadPreference.primary());
        Iterable<BasicBSONObject> members = (Iterable<BasicBSONObject>) result.get("members");

        MemberReplicationInfo primary = null;
        List<MemberReplicationInfo> secondaries = new ArrayList<MemberReplicationInfo>();
        unknownState = false;
        for (BasicBSONObject member : members) {
            ReplicaSetMemberState state = ReplicaSetMemberState.valueOf(member.getString("stateStr"));

            switch (state) {
            case PRIMARY:
                primary = new MemberReplicationInfo(member);
                break;

            case SECONDARY:
                secondaries.add(new MemberReplicationInfo(member));
                break;

            case ARBITER:
                continue;

            default:
                LOG.debug("Invalid state {} for instance {}", state, member.getString("name"));
                unknownState = true;
                break;
            }
        }

        if (primary == null) {
            LOG.debug("Can't estimate the replication lag - there's no PRIMARY instance");
        } else if (secondaries.isEmpty()) {
            LOG.debug("Can't estimate the replication lag - there's no SECONDARY instance");
        } else {
            Set<String> secondaryNames = new HashSet<String>();
            for (MemberReplicationInfo secondary : secondaries) {
                String name = secondary.name;
                Long lag = primary.getLag(secondary);
                secondaryNames.add(name);
                if (lag == null) {
                    unknownState = true;
                    continue;
                }
                if (!memberLags.containsKey(name)) {
                    memberLags.put(name, new Stat(STAT_SIZE));
                }
                memberLags.get(name).add(lag);
            }
            memberLags.keySet().retainAll(secondaryNames);
        }
    }

    private enum ReplicaSetMemberState {
        STARTUP, PRIMARY, SECONDARY, RECOVERING, STARTUP2, UNKNOWN, ARBITER, DOWN, ROLLBACK, REMOVED
    }

    private static class MemberReplicationInfo {

        private final String name;

        private final long optime;

        private final long lastHeartbeatRecv;

        public MemberReplicationInfo(BasicBSONObject member) {
            name = member.getString("name");
            optime = ((BSONTimestamp) member.get("optime")).getTime();
            lastHeartbeatRecv = member.getDate("lastHeartbeatRecv").toInstant().getEpochSecond();
        }

        public Long getLag(MemberReplicationInfo secondary) {
            if (optime >= secondary.lastHeartbeatRecv) {
                LOG.debug(
                        "Can't estimate the replication lag - there is an unprocessed operation. PRIMARY optime [{}] >= {} lastHeartbeatRecv [{}]",
                        optime, secondary.name, secondary.lastHeartbeatRecv);
                return null;
            } else if (optime > secondary.optime) {
                LOG.debug(
                        "Can't estimate the replication lag - there is an unprocessed operation. PRIMARY optime [{}] > {} optime [{}]",
                        optime, secondary.name, secondary.optime);
                return null;
            } else {
                return secondary.optime - optime;
            }
        }
    }

    private static class Stat {

        private final long[] data;

        private int i;

        private int size;

        private Long average;

        public Stat(int size) {
            this.data = new long[size];
        }

        public void add(long entry) {
            if (size < data.length) {
                size++;
            }
            data[i] = entry;
            i = (i + 1) % data.length;
            average = null;
        }

        public Long average() {
            if (average != null) {
                return average;
            }
            if (size == 0) {
                return null;
            }
            long sum = 0;
            for (int i = 0; i < size; i++) {
                sum += data[i];
            }
            return average = sum / size;
        }

        @Override
        public String toString() {
            return new StringBuilder("[avg: ").append(average).append("]").toString();
        }
    }
}