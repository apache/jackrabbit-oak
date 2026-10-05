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

/**
 * Centralized health/heartbeat tracking for Aeron consensus.
 */
@Component(service = AeronHealthService.class)
public class AeronHealthService {

    private static final long DEFAULT_HEARTBEAT_MAX_AGE_MS = 30000L;

    private volatile long lastHeartbeatTime = System.currentTimeMillis();

    public void markHeartbeat() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public long getHeartbeatAgeMs() {
        return System.currentTimeMillis() - lastHeartbeatTime;
    }

    public boolean isHeartbeatStale() {
        return getHeartbeatAgeMs() > Long.getLong("oak.cluster.heartbeat.maxAgeMs", DEFAULT_HEARTBEAT_MAX_AGE_MS);
    }

    public boolean isClusterHealthy(Cluster.Role role,
                                    java.util.function.Supplier<Boolean> quorumSupplier,
                                    java.util.function.Supplier<io.aeron.cluster.client.AeronCluster> clientSupplier) {
        if (role == null) {
            return false;
        }
        if (role == Cluster.Role.CANDIDATE) {
            return false;
        }
        if (quorumSupplier != null && !quorumSupplier.get()) {
            return false;
        }
        return true;
    }

    public String getUnhealthyReason(Cluster.Role role,
                                     java.util.function.Supplier<Boolean> quorumSupplier,
                                     java.util.function.Supplier<io.aeron.cluster.client.AeronCluster> clientSupplier) {
        if (role == null) {
            return "cluster_not_initialized";
        }
        if (role == Cluster.Role.CANDIDATE) {
            return "leader_election_in_progress";
        }
        if (quorumSupplier != null && !quorumSupplier.get()) {
            return "no_quorum";
        }
        return null;
    }
}
