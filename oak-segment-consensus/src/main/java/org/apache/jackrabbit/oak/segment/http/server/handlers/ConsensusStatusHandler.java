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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handler for consensus status (`/v1/consensus/status`).
 */
public class ConsensusStatusHandler {

    private final ServerContext context;

    public ConsensusStatusHandler(ServerContext context) {
        this.context = context;
    }

    /**
     * Handle GET /v1/consensus/status - Return comprehensive consensus state.
     */
    public void handleGetConsensusStatus(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        Map<String, Object> status = buildConsensusStatus();
        status.values().removeIf(v -> v == null);
        response.getWriter().write(JsonOutputUtil.toJson(status));
    }

    /**
     * Handle GET /v1/consensus/leader - Return canonical leader-resolution data.
     */
    public void handleGetConsensusLeader(HttpServletResponse response) throws IOException {
        handleGetConsensusLeader(false, response);
    }

    /**
     * Handle GET /v1/consensus/leader. With {@code localOnly} the answer comes from this node's local
     * knowledge only and never triggers peer polling; peers use it for leader discovery so that two
     * followers cannot poll each other recursively. The response shape is the same either way.
     */
    public void handleGetConsensusLeader(boolean localOnly, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        Map<String, Object> leader = buildConsensusLeaderStatus(localOnly);
        leader.values().removeIf(v -> v == null);
        response.getWriter().write(JsonOutputUtil.toJson(leader));
    }

    private Map<String, Object> buildConsensusStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("contractVersion", "consensus.status.v1");

        if (context.aeronConsensusEngine != null) {
            status.put("consensusType", "aeron-cluster");
            status.put("currentRole", context.aeronConsensusEngine.getCurrentRole().name());
            status.put("isLeader", context.aeronConsensusEngine.isLeader());

            String currentLeader = context.aeronConsensusEngine.getCurrentLeader();
            if (currentLeader != null) {
                status.put("currentLeader", currentLeader);
            }

            status.put("currentEpoch", context.aeronConsensusEngine.getCurrentEpoch());
            status.put("currentTerm", context.aeronConsensusEngine.getCurrentTerm());
            status.put("reachableValidators", context.aeronConsensusEngine.getReachableValidatorCount());
            status.put("allFollowers", context.aeronConsensusEngine.getAllFollowers());
            status.put("ethereumEpoch", context.aeronConsensusEngine.getCurrentEthereumEpoch());
        } else {
            status.put("consensusType", "none");
            status.put("currentRole", "STANDALONE");
        }

        return status;
    }

    private Map<String, Object> buildConsensusLeaderStatus(boolean localOnly) {
        Map<String, Object> leader = new LinkedHashMap<>();
        leader.put("contractVersion", "consensus.leader.v1");

        if (context.aeronConsensusEngine != null) {
            String currentLeader = localOnly
                ? context.aeronConsensusEngine.getCurrentLeaderHint()
                : context.aeronConsensusEngine.getCurrentLeader();
            leader.put("consensusType", "aeron-cluster");
            leader.put("currentRole", context.aeronConsensusEngine.getCurrentRole().name());
            leader.put("isLeader", context.aeronConsensusEngine.isLeader());
            leader.put("currentTerm", context.aeronConsensusEngine.getCurrentTerm());
            leader.put("currentLeader", currentLeader);
            leader.put("leaderKnown", currentLeader != null && !currentLeader.isEmpty());
        } else {
            leader.put("consensusType", "none");
            leader.put("currentRole", "STANDALONE");
            leader.put("isLeader", false);
            leader.put("leaderKnown", false);
        }

        return leader;
    }
}
