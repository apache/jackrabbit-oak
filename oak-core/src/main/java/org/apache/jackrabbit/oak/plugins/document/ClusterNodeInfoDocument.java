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

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState;

/**
 * A document storing cluster node info. See also {@link ClusterNodeInfo}.
 */
public class ClusterNodeInfoDocument extends Document {

    /**
     * All ClusterNodeInfoDocument ID value would be greater than this value
     * It can be used as startKey in DocumentStore#query methods
     */
    private static final String MIN_ID_VALUE = "0";

    /**
     * All ClusterNodeInfoDocument ID value would be less than this value
     * It can be used as endKey in DocumentStore#query methods
     */
    private static final String MAX_ID_VALUE = "a";

    public long getLeaseEndTime(){
        return checkNotNull((Long) get(ClusterNodeInfo.LEASE_END_KEY), "Lease End Time not set");
    }

    public boolean isActive(){
        return getState() == ClusterNodeState.ACTIVE;
    }

    public boolean isBeingRecovered(){
        return getRecoveryState() == RecoverLockState.ACQUIRED;
    }

    public int getClusterId() {
        return Integer.parseInt(getId());
    }

    /**
     * Returns all cluster node info documents currently available in the given
     * document store.
     *
     * @param store the document store.
     * @return list of cluster node info documents.
     */
    public static List<ClusterNodeInfoDocument> all(DocumentStore store) {
        // keys between "0" and "a" includes all possible numbers
        return store.query(Collection.CLUSTER_NODES,
                MIN_ID_VALUE, MAX_ID_VALUE, Integer.MAX_VALUE);
    }

    //-----------------------< internal >---------------------------------------

    private ClusterNodeState getState(){
        return ClusterNodeState.fromString((String) get(ClusterNodeInfo.STATE));
    }

    private RecoverLockState getRecoveryState(){
        return RecoverLockState.fromString((String) get(ClusterNodeInfo.REV_RECOVERY_LOCK));
    }
}
