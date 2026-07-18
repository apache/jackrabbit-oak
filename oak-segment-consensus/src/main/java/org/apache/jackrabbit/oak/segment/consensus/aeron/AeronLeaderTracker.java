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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.cluster.service.Cluster;
import org.osgi.service.component.annotations.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks leadership changes and delegates leader discovery notifications.
 */
@Component(service = AeronLeaderTracker.class)
public class AeronLeaderTracker {

    private static final int MAX_HISTORY_ENTRIES = 100;

    private final LeaderDiscoveryService leaderDiscoveryService;
    private final List<LeadershipChange> leadershipHistory = new CopyOnWriteArrayList<>();

    public AeronLeaderTracker(LeaderDiscoveryService leaderDiscoveryService) {
        this.leaderDiscoveryService = leaderDiscoveryService;
    }

    public void invalidateCache() {
        if (leaderDiscoveryService != null) {
            leaderDiscoveryService.invalidateCache();
        }
    }

    public void notifyBecameLeader(int memberId) {
        if (leaderDiscoveryService != null) {
            leaderDiscoveryService.notifyBecameLeader(memberId);
        }
    }

    public void notifyLostLeadership() {
        if (leaderDiscoveryService != null) {
            leaderDiscoveryService.notifyLostLeadership();
        }
    }

    public void recordChange(Cluster.Role newRole,
                             Cluster.Role previousRole,
                             int term,
                             int memberId,
                             String memberUrl,
                             long timestamp) {
        recordChange(newRole, previousRole, term, memberId, memberUrl, timestamp, -1L);
    }

    public void recordChange(Cluster.Role newRole,
                             Cluster.Role previousRole,
                             int term,
                             int memberId,
                             String memberUrl,
                             long timestamp,
                             long clusterTime) {
        leadershipHistory.add(new LeadershipChange(
            timestamp,
            clusterTime,
            newRole,
            previousRole,
            term,
            memberId,
            memberUrl
        ));
        if (leadershipHistory.size() > MAX_HISTORY_ENTRIES) {
            leadershipHistory.remove(0);
        }
    }

    public List<LeadershipChange> getLeadershipHistory(int limit) {
        List<LeadershipChange> result = new ArrayList<>(leadershipHistory);
        Collections.reverse(result);
        if (limit > 0 && result.size() > limit) {
            return result.subList(0, limit);
        }
        return result;
    }
}
