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
import java.util.function.Predicate;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState;
import static org.apache.jackrabbit.oak.plugins.document.Revision.fromString;
import static org.apache.jackrabbit.oak.plugins.document.Revision.getTimestampDifference;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    /**
     * The timestamp when this document was created.
     */
    private final long created = Revision.getCurrentTimestamp();

    /**
     * @return the timestamp when this document was created.
     */
    public long getCreated() {
        return created;
    }

    public long getLeaseEndTime(){
        return requireNonNull((Long) get(ClusterNodeInfo.LEASE_END_KEY), "Lease End Time not set");
    }

    /**
     * @return the time when this cluster node was started or {@code -1} if not
     *          available.
     */
    public long getStartTime() {
        Long startTime = (Long) get(ClusterNodeInfo.START_TIME_KEY);
        if (startTime == null) {
            startTime = -1L;
        }
        return startTime;
    }

    /**
     * @return the Runtime ID for this cluster node.
     */
    @Nullable
    public String getRuntimeId() {
        return (String) get(ClusterNodeInfo.RUNTIME_ID_KEY);
    }

    public boolean isActive(){
        return getState() == ClusterNodeState.ACTIVE;
    }

    /**
     * @return {@code true} if the recovery lock state is
     *          {@link RecoverLockState#ACQUIRED ACQUIRED}.
     */
    public boolean isBeingRecovered(){
        return getRecoveryState() == RecoverLockState.ACQUIRED;
    }

    /**
     * Check if _lastRev recovery is needed for cluster node info document.
     * Returns {@code true} if both of the below conditions are {@code true}:
     * <ul>
     *     <li>State is Active</li>
     *     <li>Current time is past the leaseEnd time or there is a recovery
     *          lock on the cluster node info document</li>
     * </ul>
     * @param currentTimeMillis the current time in milliseconds since the start
     *                          start of the epoch.
     */
    public boolean isRecoveryNeeded(long currentTimeMillis) {
        return isActive() &&
                (currentTimeMillis - getLeaseEndTime() > ClusterNodeInfo.getRecoveryDelayMillis() ||
                        isBeingRecovered());
    }

    /**
     * Returns {@code true} if the cluster node represented by this document
     * is currently being recovered by the given {@code clusterId}.
     *
     * @param clusterId the id of a cluster node.
     * @return {@code true} if being recovered by the given id; {@code false}
     *          otherwise.
     */
    public boolean isBeingRecoveredBy(int clusterId) {
        return Long.valueOf(clusterId).equals(getRecoveryBy());
    }

    /**
     * @return the id of the cluster node performing recovery or {@code null} if
     *          currently not set.
     */
    @Nullable
    public Long getRecoveryBy() {
        return (Long) get(ClusterNodeInfo.REV_RECOVERY_BY);
    }

    public int getClusterId() {
        return Integer.parseInt(getId());
    }

    @Override
    public String toString() {
        return format();
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

    /**
     * the root-revision of the last background write (of unsaved modifications)
     **/
    public String getLastWrittenRootRev() {
        return (String) get(ClusterNodeInfo.LAST_WRITTEN_ROOT_REV_KEY);
    }

    /**
     * Is the cluster node marked as invisible
     * @return {@code true} if invisible; {@code false}
     *         otherwise.
     */
    public boolean isInvisible() {
        Boolean invisible = (Boolean) get(ClusterNodeInfo.INVISIBLE);
        return invisible != null ? invisible : false;
    }

    /**
     * Method to create {@link Predicate} to filter revisions
     *
     * @return {@link Predicate} to filter revisions older than lastWrittenRootRev
     */
    // VisibleForTesting
    @NotNull
    Predicate<Revision> isOlderThanLastWrittenRootRevPredicate() {
        return r -> nonNull(getLastWrittenRootRev()) && getTimestampDifference(r, fromString(getLastWrittenRootRev())) < 0;
    }
}
