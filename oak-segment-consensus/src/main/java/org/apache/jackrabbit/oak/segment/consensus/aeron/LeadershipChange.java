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

/**
 * Records a leadership role transition in the Aeron cluster.
 */
public class LeadershipChange {
    public final long timestamp;
    public final long clusterTime;
    public final Cluster.Role newRole;
    public final Cluster.Role previousRole;
    public final int term;
    public final int memberId;
    public final String memberUrl;

    public LeadershipChange(long timestamp, Cluster.Role newRole, Cluster.Role previousRole,
                            int term, int memberId, String memberUrl) {
        this(timestamp, -1L, newRole, previousRole, term, memberId, memberUrl);
    }

    public LeadershipChange(long timestamp, long clusterTime,
                            Cluster.Role newRole, Cluster.Role previousRole,
                            int term, int memberId, String memberUrl) {
        this.timestamp = timestamp;
        this.clusterTime = clusterTime;
        this.newRole = newRole;
        this.previousRole = previousRole;
        this.term = term;
        this.memberId = memberId;
        this.memberUrl = memberUrl;
    }
}
