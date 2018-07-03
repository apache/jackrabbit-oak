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

import java.util.Comparator;

/**
 * Comparator for {@link ClusterNodeInfo} instances based on a given environment
 * defined by a {@code machineId} and {@code instanceId}. The comparator orders
 * cluster node info with a matching environment before others that do not match
 * and then compares the clusterId, lower values first.
 */
class ClusterNodeInfoComparator implements Comparator<ClusterNodeInfo> {

    private static final Comparator<Boolean> BOOLEAN_REVERSED = Comparator.comparing(Boolean::booleanValue).reversed();

    private final String machineId;
    private final String instanceId;

    ClusterNodeInfoComparator(String machineId,
                              String instanceId) {
        this.machineId = machineId;
        this.instanceId = instanceId;
    }

    @Override
    public int compare(ClusterNodeInfo info1,
                       ClusterNodeInfo info2) {
        // first compare whether the environment matches
        return Comparator.comparing(this::matchesEnvironment, BOOLEAN_REVERSED)
                // then compare the clusterIds
                .thenComparingInt(ClusterNodeInfo::getId).compare(info1, info2);
    }

    private boolean matchesEnvironment(ClusterNodeInfo info) {
        return machineId.equals(info.getMachineId())
                && instanceId.equals(info.getInstanceId());
    }
}
