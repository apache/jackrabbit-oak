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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class AeronClusterStateView {

    @FunctionalInterface
    interface UrlMatcher {
        boolean matches(String left, String right);
    }

    private final String selfUrl;
    private final List<String> peerUrls;
    private final Map<Integer, String> nodeIdToUrl;
    private final UrlMatcher urlMatcher;

    AeronClusterStateView(String selfUrl,
                          List<String> peerUrls,
                          Map<Integer, String> nodeIdToUrl,
                          UrlMatcher urlMatcher) {
        this.selfUrl = selfUrl;
        this.peerUrls = peerUrls;
        this.nodeIdToUrl = nodeIdToUrl;
        this.urlMatcher = urlMatcher;
    }

    Map<String, Object> buildNativeClusterState(Cluster cluster,
                                                String leaderUrl,
                                                String walletAddress,
                                                String publicKey,
                                                int currentTerm,
                                                int currentEpoch,
                                                int currentEthereumEpoch) {
        Map<String, Object> state = new HashMap<>();
        Cluster.Role role = cluster.role();
        state.put("role", role.name());
        state.put("isLeader", role == Cluster.Role.LEADER);

        int selfMemberId = cluster.memberId();
        if (selfMemberId < 0) {
            selfMemberId = findNodeIdByUrl(selfUrl);
        }
        state.put("memberId", selfMemberId);
        state.put("clusterTime", cluster.time());
        state.put("logPosition", cluster.logPosition());
        state.put("term", currentTerm);
        state.put("epoch", currentEpoch);
        state.put("ethereumEpoch", currentEthereumEpoch);

        List<Map<String, Object>> members = new ArrayList<>();

        Map<String, Object> selfInfo = new HashMap<>();
        selfInfo.put("memberId", selfMemberId);
        selfInfo.put("url", selfUrl);
        selfInfo.put("role", role.name());
        selfInfo.put("status", "ACTIVE");
        if (walletAddress != null) {
            selfInfo.put("walletAddress", walletAddress);
        }
        if (publicKey != null) {
            selfInfo.put("publicKey", publicKey);
        }
        members.add(selfInfo);

        if (peerUrls != null) {
            for (String peerUrl : peerUrls) {
                if (matches(peerUrl, selfUrl)) {
                    continue;
                }
                Map<String, Object> memberInfo = new HashMap<>();
                memberInfo.put("memberId", findNodeIdByUrl(peerUrl));
                memberInfo.put("url", peerUrl);
                memberInfo.put("role", matches(peerUrl, leaderUrl) ? "LEADER" : "FOLLOWER");
                memberInfo.put("status", "ACTIVE");
                members.add(memberInfo);
            }
        }

        state.put("members", members);
        state.put("memberCount", members.size());
        state.put("currentLeader", leaderUrl);
        return state;
    }

    int resolveLeaderMemberId(Cluster cluster, String currentLeader) {
        if (cluster != null && cluster.role() == Cluster.Role.LEADER) {
            return cluster.memberId();
        }
        return findNodeIdByUrl(currentLeader);
    }

    Map<String, Object> buildReplicationLagStatus(Cluster cluster, long leaderLogPosition, long lag) {
        Map<String, Object> status = new HashMap<>();
        boolean measurementAvailable = lag >= 0;
        status.put("role", cluster.role().name());
        status.put("myLogPosition", cluster.logPosition());
        status.put("leaderLogPosition", measurementAvailable ? leaderLogPosition : null);
        status.put("replicationLag", measurementAvailable ? lag : null);
        status.put("lagThreshold", 1000L);
        status.put("measurementAvailable", measurementAvailable);
        status.put("healthStatus", measurementAvailable ? (lag < 1000 ? "HEALTHY" : "LAGGING") : "UNKNOWN");
        status.put("healthy", measurementAvailable ? lag < 1000 : null);
        if (!measurementAvailable) {
            status.put("reason", "leader_log_position_unknown");
        }
        return status;
    }

    int findNodeIdByUrl(String url) {
        if (url == null || nodeIdToUrl == null) {
            return -1;
        }
        for (Map.Entry<Integer, String> entry : nodeIdToUrl.entrySet()) {
            if (matches(entry.getValue(), url)) {
                return entry.getKey();
            }
        }
        return -1;
    }

    private boolean matches(String left, String right) {
        return left != null && right != null && urlMatcher.matches(left, right);
    }
}
