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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    public long getLagEstimation() {
        return lastEstimatedValue;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        while (!stop) {
            CommandResult result = adminDb.command("replSetGetStatus", ReadPreference.primary());
            updateStats((Iterable<BasicBSONObject>) result.get("members"));
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

    long estimate() {
        if (unknownState || memberLags.isEmpty()) {
            return maxReplicationLagMillis;
        } else {
            LOG.debug("Average lags for the instances: {}", memberLags);
            Long max = null;
            boolean allStatsPresent = true;
            for (Entry<String, Stat> e : memberLags.entrySet()) {
                Long avg = e.getValue().average();
                if (avg == null) {
                    LOG.debug("There's no data for instance {}", e.getKey());
                    allStatsPresent = false;
                }
                if (max == null || max < avg) {
                    max = avg;
                }
            }
            if (allStatsPresent) {
                LOG.debug("Estimated maximum lag is: {}", max);
                return max;
            } else {
                return maxReplicationLagMillis;
            }
        }
    }

    /**
     * Read the replication lag for each member by comparing PRIMARY and
     * SECONDARY nodes last optime value.
     */
    void updateStats(Iterable<BasicBSONObject> members) {
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
                secondaryNames.add(name);

                Stat stat = memberLags.get(name);
                if (stat == null) {
                    memberLags.put(name, stat = new Stat(STAT_SIZE));
                }

                Long lag = primary.getLag(secondary);
                if (lag == null) {
                    unknownState = true;
                    stat.removeOldest();
                } else {
                    stat.add(lag);
                }
            }
            memberLags.keySet().retainAll(secondaryNames);
        }
    }

    enum ReplicaSetMemberState {
        STARTUP, PRIMARY, SECONDARY, RECOVERING, STARTUP2, UNKNOWN, ARBITER, DOWN, ROLLBACK, REMOVED
    }

    static class MemberReplicationInfo {

        private final String name;

        private final long optime;

        private final long lastHeartbeatRecv;

        public MemberReplicationInfo(BasicBSONObject member) {
            name = member.getString("name");
            optime = 1000l * ((BSONTimestamp) member.get("optime")).getTime();
            lastHeartbeatRecv = member.getDate("lastHeartbeatRecv").getTime();
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

        private final Deque<Long> data;

        private final int size;

        private Long average;

        public Stat(int size) {
            this.data = new LinkedList<Long>();
            this.size = size;
        }

        public void add(long entry) {
            if (data.size() == size) {
                data.removeFirst();
            }
            data.addLast(entry);
            average = null;
        }

        public void removeOldest() {
            data.removeFirst();
            average = null;
        }

        public Long average() {
            if (average != null) {
                return average;
            }
            if (data.isEmpty()) {
                return null;
            }
            long sum = 0;
            for (Long i : data) {
                sum += i;
            }
            return average = sum / data.size();
        }

        @Override
        public String toString() {
            return new StringBuilder("[avg: ").append(average).append("]").toString();
        }
    }
}