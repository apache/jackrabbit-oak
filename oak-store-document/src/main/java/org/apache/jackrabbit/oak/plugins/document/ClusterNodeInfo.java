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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState.ACTIVE;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState.NONE;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState.ACQUIRED;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.asISO8601;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.document.spi.lease.LeaseFailureHandler;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information about a cluster node.
 */
public class ClusterNodeInfo {

    private static final String LEASE_CHECK_FAILED_MSG = "This oak instance failed to update "
            + "the lease in time and can therefore no longer access this DocumentNodeStore.";

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodeInfo.class);

    /**
     * The prefix for random (non-reusable) keys.
     */
    private static final String RANDOM_PREFIX = "random:";

    /**
     * The machine id.
     */
    private static final String MACHINE_ID_KEY = "machine";

    /**
     * The Oak version.
     */
    static final String OAK_VERSION_KEY = "oakVersion";

    /**
     * The unique instance id within this machine (the current working directory
     * if not set).
     */
    private static final String INSTANCE_ID_KEY = "instance";

    /**
     * The end of the lease.
     */
    public static final String LEASE_END_KEY = "leaseEnd";

    /**
     * The time a recovery was done, if the last shutdown required a recover - not set otherwise.
     */
    public static final String RECOVERY_TIME_KEY = "recoveryTime";

    /**
     * The start time.
     */
    public static final String START_TIME_KEY = "startTime";

    /**
     * The key for the root-revision of the last background write (of unsaved
     * modifications) - that is: the last root-revision written by the instance
     * in case of a clear shutdown or via recovery of another instance in case
     * of a crash
     */
    public static final String LAST_WRITTEN_ROOT_REV_KEY = "lastWrittenRootRev";

    /**
     * The state of the cluster. On proper shutdown the state should be cleared.
     *
     * @see org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.ClusterNodeState
     */
    public static final String STATE = "state";
    
    /**
     * The broadcast id. If the broadcasting cache is used, a new id is set after startup.
     */
    public static final String BROADCAST_ID = "broadcastId";

    /**
     * The broadcast listener (host:port). If the broadcasting cache is used, this is set after startup.
     */
    public static final String BROADCAST_LISTENER = "broadcastListener";

    public static enum ClusterNodeState {
        NONE,
        /**
         * Indicates that cluster node is currently active
         */
        ACTIVE;

        static ClusterNodeState fromString(String state) {
            if (state == null) {
                return NONE;
            }
            return valueOf(state);
        }
    }

    /**
     * Flag to indicate whether the _lastRev recovery is in progress.
     *
     * @see RecoverLockState
     */
    public static final String REV_RECOVERY_LOCK = "recoveryLock";

    public static enum RecoverLockState {
        NONE,
        /**
         * _lastRev recovery in progress
         */
        ACQUIRED;

        static RecoverLockState fromString(String state) {
            if (state == null) {
                return NONE;
            }
            return valueOf(state);
        }
    }

    /**
     * Flag indicating which cluster node is running the recovery.
     */
    public static final String REV_RECOVERY_BY = "recoveryBy";

    /**
     * Additional info, such as the process id, for support.
     */
    private static final String INFO_KEY = "info";

    /**
     * The read/write mode key. Specifies the read/write preference to be used with
     * DocumentStore
     */
    private static final String READ_WRITE_MODE_KEY = "readWriteMode";

    /**
     * Key for invisible flag
     */
    public static final String INVISIBLE = "invisible";

    /**
     * Runtime UUID (generated unique ID for this instance)
     */
    public static final String RUNTIME_ID_KEY = "runtime_id";

    /**
     * The unique machine id (the MAC address if available).
     */
    private static final String MACHINE_ID = getHardwareMachineId();

    /**
     * The process id (if available).
     */
    private static final long PROCESS_ID = getProcessId();

    /**
     * The current working directory.
     * Note: marked protected non-final for testing purpose only.
     */
    protected static String WORKING_DIR = System.getProperty("user.dir", "");

    /**
     * <b>Only Used For Testing</b>
     */
    private static Clock clock = Clock.SIMPLE;

    /** OAK-3398 : default lease duration 120sec **/
    private static final int DEFAULT_LEASE_DURATION_SEC = SystemPropertySupplier.create("oak.documentMK.leaseDurationSeconds", 120)
            .loggingTo(LOG).validateWith(value -> value >= 0)
            .formatSetMessage((name, value) -> String.format("Lease duration set to: %ss (using system property %s)", value, name)).get();
    public static final int DEFAULT_LEASE_DURATION_MILLIS = 1000 * DEFAULT_LEASE_DURATION_SEC;

    /** OAK-3398 : default update interval 10sec **/
    public static final int DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS = 1000 * 10;

    /** OAK-3398 : default failure margin 20sec before actual lease timeout
     * (note that OAK-3399 / MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE eats
     * off another few seconds from this margin, by default 5sec,
     * so the actual default failure-margin is down to 15sec - and that is high-noon!)
     */
    public static final int DEFAULT_LEASE_FAILURE_MARGIN_MILLIS = 1000 * 20;

    public static final boolean DEFAULT_LEASE_CHECK_DISABLED = SystemPropertySupplier
            .create("oak.documentMK.disableLeaseCheck", Boolean.FALSE).loggingTo(LOG).get();

    /**
     * Default lease check mode is strict, unless disabled via system property.
     */
    static final LeaseCheckMode DEFAULT_LEASE_CHECK_MODE =
            DEFAULT_LEASE_CHECK_DISABLED ? LeaseCheckMode.DISABLED : LeaseCheckMode.STRICT;

    /** OAK-3399 : max number of times we're doing a 1sec retry loop just before declaring lease failure **/
    private static final int MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE = 5;

    /** OAK-10622 : millis to prevent reuse of clusterId if it crashed and thus required a recover */
    static final int DEFAULT_REUSE_DELAY_AFTER_RECOVERY_MILLIS = 0;

    /** OAK-10281 : default millis to delay a recovery after a lease timeout */
    static final long DEFAULT_RECOVERY_DELAY_MILLIS = 0;

    /**
     * Actual millis to delay a recovery after a lease timeout.
     * <p>
     * Initialized by DocumentNodeStore constructor.
     */
    static long recoveryDelayMillis = DEFAULT_RECOVERY_DELAY_MILLIS;

    /**
     * The Oak version.
     */
    private static final String OAK_VERSION = getModuleVersion();

    /**
     * The number of milliseconds for a lease (2 minute by default, and
     * initially).
     */
    private long leaseTime = DEFAULT_LEASE_DURATION_MILLIS;

    /**
     * The number of milliseconds after which a lease will be updated
     * (should not be every second as that would increase number of
     * writes towards DocumentStore considerably - but it should also
     * not be too low as that would eat into the lease duration on average.
     */
    private long leaseUpdateInterval = DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS;

    /**
     * The number of milliseconds that a lease must still be valid
     * before prematurely declaring it as failed. The default is 20sec.
     * The idea of declaring a lease as failed before it actually failed
     * is to avoid a race condition where the local instance assumes
     * things are all fine but another instance in the cluster will
     * 'in the same moment' declare it as failed. The lease should be
     * checked every second and updated after 10sec, so it should always
     * have a validity of at least 110sec - if that's down to this margin
     * of 20sec then things are not good and we have to give up.
     */
    private long leaseFailureMargin = DEFAULT_LEASE_FAILURE_MARGIN_MILLIS;

    /**
     * The assigned cluster id.
     */
    private final int id;

    /**
     * The machine id.
     */
    private final String machineId;

    /**
     * The instance id.
     */
    private final String instanceId;

    /**
     * The document store that is used to renew the lease.
     */
    private final DocumentStore store;

    /**
     * The time (in milliseconds UTC) where this instance was started.
     */
    private final long startTime;

    /**
     * A unique id.
     */
    private final String uuid = UUID.randomUUID().toString();

    /**
     * The time (in milliseconds UTC) where the lease of this instance ends.
     */
    private volatile long leaseEndTime;

    /**
     * The read/write mode.
     */
    private String readWriteMode;

    /**
     * The state of the cluster node.
     */
    private ClusterNodeState state = ACTIVE;

    /**
     * OAK-2739 / OAK-3397 : once a lease check turns out negative, this flag
     * is set to prevent any further checks to succeed. Also, only the first
     * one to change this flag will take the appropriate action that results
     * from a failed leaseCheck (which is currently to stop oak-store-document bundle)
     */
    private boolean leaseCheckFailed = false;

    /**
     * Default lease check mode is strict, unless disabled by
     */
    private LeaseCheckMode leaseCheckMode = DEFAULT_LEASE_CHECK_MODE;

    /**
     * In memory flag indicating that this ClusterNode is entry is new and is being added to
     * DocumentStore for the first time
     *
     * If false then it indicates that a previous entry for current node existed and that is being
     * reused
     */
    private boolean newEntry;

    /**
     * OAK-3397 / OAK-3400 : the LeaseFailureHandler is the one that actually
     * stops the oak-store-document bundle (or does something else if necessary)
     */
    private LeaseFailureHandler leaseFailureHandler;

    /**
     * Flag to indicate this node is invisible to cluster view and thus recovery.
     */
    private boolean invisible;

    /**
     * Unique ID generated at Runtime.
     */
    private final String runtimeId;

    private ClusterNodeInfo(int id, DocumentStore store, String machineId,
                            String instanceId, boolean newEntry, boolean invisible) {
        this.id = id;
        this.startTime = getCurrentTime();
        this.leaseEndTime = this.startTime +leaseTime;
        this.store = store;
        this.machineId = machineId;
        this.instanceId = instanceId;
        this.newEntry = newEntry;
        this.invisible = invisible;
        this.runtimeId = UUIDUtils.generateUUID();
    }

    void setLeaseCheckMode(@NotNull LeaseCheckMode mode) {
        this.leaseCheckMode = requireNonNull(mode);
    }

    LeaseCheckMode getLeaseCheckMode() {
        return leaseCheckMode;
    }
    
    public int getId() {
        return id;
    }

    String getMachineId() {
        return machineId;
    }

    String getInstanceId() {
        return instanceId;
    }

    String getRuntimeId() {
        return runtimeId;
    }

    boolean isInvisible() {
        return invisible;
    }

    /**
     * Create a cluster node info instance to be utilized for read only access
     * to underlying store.
     *
     * @param store the document store.
     * @return the cluster node info
     */
    public static ClusterNodeInfo getReadOnlyInstance(DocumentStore store) {
        return new ClusterNodeInfo(0, store, MACHINE_ID, WORKING_DIR, true, true) {
            @Override
            public void dispose() {
            }

            @Override
            public long getLeaseEndTime() {
                return Long.MAX_VALUE;
            }

            @Override
            public void performLeaseCheck() {
            }

            @Override
            public boolean renewLease() {
                return false;
            }

            @Override
            public void setInfo(Map<String, String> info) {}

            @Override
            public void setLeaseFailureHandler(LeaseFailureHandler leaseFailureHandler) {}
        };
    }

    /**
     * Get or create a cluster node info instance for the store.
     *
     * @param store the document store (for the lease)
     * @param recoveryHandler the recovery handler to call for a clusterId with
     *                        an expired lease.
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @param configuredClusterId the configured cluster id (or 0 for dynamic assignment)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store,
        RecoveryHandler recoveryHandler,
        String machineId,
        String instanceId,
        int configuredClusterId) {

        return getInstance(store, recoveryHandler, machineId, instanceId, configuredClusterId, false);
    }

    /**
     * Get or create a cluster node info instance for the store.
     *
     * @param store the document store (for the lease)
     * @param recoveryHandler the recovery handler to call for a clusterId with
     *                        an expired lease.
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @param configuredClusterId the configured cluster id (or 0 for dynamic assignment)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store,
                                              RecoveryHandler recoveryHandler,
                                              String machineId,
                                              String instanceId,
                                              int configuredClusterId,
                                              boolean invisible) {
        return getInstance(store, recoveryHandler, machineId, instanceId, configuredClusterId,
                invisible, DEFAULT_REUSE_DELAY_AFTER_RECOVERY_MILLIS);
    }

    /**
     * Get or create a cluster node info instance for the store.
     *
     * @param store the document store (for the lease)
     * @param recoveryHandler the recovery handler to call for a clusterId with
     *                        an expired lease.
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @param configuredClusterId the configured cluster id (or 0 for dynamic assignment)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store,
                                              RecoveryHandler recoveryHandler,
                                              String machineId,
                                              String instanceId,
                                              int configuredClusterId,
                                              boolean invisible,
                                              long reuseAfterRecoveryMillis) {
        // defaults for machineId and instanceID
        if (machineId == null) {
            machineId = MACHINE_ID;
        }
        if (instanceId == null) {
            instanceId = WORKING_DIR;
        }

        int retries = 10;
        for (int i = 0; i < retries; i++) {
            Map.Entry<ClusterNodeInfo, Long> suggestedClusterNode =
                    createInstance(store, recoveryHandler, machineId,
                            instanceId, configuredClusterId, i == 0, invisible, reuseAfterRecoveryMillis);
            ClusterNodeInfo clusterNode = suggestedClusterNode.getKey();
            Long currentStartTime = suggestedClusterNode.getValue();
            String key = String.valueOf(clusterNode.id);
            UpdateOp update = new UpdateOp(key, clusterNode.newEntry);
            update.set(MACHINE_ID_KEY, clusterNode.machineId);
            update.set(INSTANCE_ID_KEY, clusterNode.instanceId);
            update.set(LEASE_END_KEY, clusterNode.leaseEndTime);
            update.set(RECOVERY_TIME_KEY, null);
            update.set(START_TIME_KEY, clusterNode.startTime);
            update.set(INFO_KEY, clusterNode.toString());
            update.set(STATE, ACTIVE.name());
            update.set(OAK_VERSION_KEY, OAK_VERSION);
            update.set(INVISIBLE, invisible);
            update.set(RUNTIME_ID_KEY, clusterNode.runtimeId);

            ClusterNodeInfoDocument before = null;
            final boolean success;
            if (clusterNode.newEntry) {
                // For new entry do a create. This ensures that if two nodes
                // create entry with same id then only one would succeed
                success = store.create(Collection.CLUSTER_NODES, Collections.singletonList(update));
            } else {
                // remember how the entry looked before the update
                before = store.find(Collection.CLUSTER_NODES, key);

                // perform a conditional update with a check on the startTime
                // field. If there are competing cluster nodes trying to acquire
                // the same inactive clusterId, only one of them will succeed.
                update.equals(START_TIME_KEY, currentStartTime);
                // ensure some other conditions
                // 1) must not be active
                update.notEquals(STATE, ACTIVE.name());
                // 2) must not have a recovery lock
                update.notEquals(REV_RECOVERY_LOCK, ACQUIRED.name());

                success = store.findAndUpdate(Collection.CLUSTER_NODES, update) != null;
            }

            if (success) {
                logClusterIdAcquired(clusterNode, before);
                return clusterNode;
            }
            LOG.info("Collision while acquiring clusterId {}. Retrying...",
                    clusterNode.getId());
        }
        throw new DocumentStoreException("Could not get cluster node info (tried " + retries + " times)");
    }

    private static Map.Entry<ClusterNodeInfo, Long> createInstance(DocumentStore store,
                                                                   RecoveryHandler recoveryHandler,
                                                                   String machineId,
                                                                   String instanceId,
                                                                   int configuredClusterId,
                                                                   boolean waitForLease,
                                                                   boolean invisible,
                                                                   long reuseAfterRecoveryMillis) {

        long now = getCurrentTime();
        int maxId = 0;

        ClusterNodeInfoDocument alreadyExistingConfigured = null;
        String reuseFailureReason = "";
        List<ClusterNodeInfoDocument> list = ClusterNodeInfoDocument.all(store);
        Map<Integer, Long> startTimes = new HashMap<>();
        SortedSet<ClusterNodeInfo> candidates = new TreeSet<>(
                new ClusterNodeInfoComparator(machineId, instanceId));

        for (ClusterNodeInfoDocument doc : list) {

            String key = doc.getId();

            int id;
            try {
                id = doc.getClusterId();
            } catch (Exception e) {
                LOG.debug("Skipping cluster node info document {} because ID is invalid", key);
                continue;
            }

            maxId = Math.max(maxId, id);

            // cannot use an entry without start time
            if (doc.getStartTime() == -1) {
                reuseFailureReason = reject(id,
                        "Cluster node entry does not have a startTime. ");
                continue;
            }

            // if a cluster id was configured: check that and abort if it does
            // not match
            if (configuredClusterId != 0) {
                if (configuredClusterId != id) {
                    continue;
                } else {
                    alreadyExistingConfigured = doc;
                }
            }

            Long leaseEnd = (Long) doc.get(LEASE_END_KEY);
            String mId = "" + doc.get(MACHINE_ID_KEY);
            String iId = "" + doc.get(INSTANCE_ID_KEY);

            // handle active clusterId with valid lease and no recovery lock
            // -> potentially wait for lease if machine and instance id match
            if (leaseEnd != null
                    && leaseEnd > now
                    && !doc.isRecoveryNeeded(now)) {
                // wait if (a) instructed to, and (b) also the remaining time
                // time is not much bigger than the lease interval (in which
                // case something is very very wrong anyway)
                if (waitForLease && (leaseEnd - now) < (DEFAULT_LEASE_DURATION_MILLIS + 5000) && mId.equals(machineId)
                        && iId.equals(instanceId)) {
                    boolean worthRetrying = waitForLeaseExpiry(store, doc, leaseEnd, machineId, instanceId);
                    if (worthRetrying) {
                        return createInstance(store, recoveryHandler, machineId, instanceId, configuredClusterId, false, invisible, reuseAfterRecoveryMillis);
                    }
                }

                reuseFailureReason = reject(id,
                        "leaseEnd " + leaseEnd + " > " + now + " - " + (leaseEnd - now) + "ms in the future");
                continue;
            }

            // if we get here the clusterId either:
            // 1) is inactive
            // 2) needs recovery
            if (doc.isRecoveryNeeded(now)) {
                if (mId.equals(machineId) && iId.equals(instanceId)) {
                    // this id matches our environment and has an expired lease
                    // use it after a successful recovery
                    if (!recoveryHandler.recover(id)) {
                        reuseFailureReason = reject(id,
                                "needs recovery and was unable to perform it myself - now = " + getCurrentTime()); // OAK-10559 : temporary, to be removed asap
                        continue;
                    }
                } else {
                    // a different machine or instance
                    reuseFailureReason = reject(id,
                            "needs recovery and machineId/instanceId do not match: " +
                                    mId + "/" + iId + " != " + machineId + "/" + instanceId);
                    continue;
                }
            }

            // if we get here the cluster node entry is inactive. if recovery
            // was needed, then it was successful

            if (reuseAfterRecoveryMillis > 0) {
                Long lastRecoveryTime = (Long) doc.get(RECOVERY_TIME_KEY);
                if (lastRecoveryTime != null) {
                    long diff = now - lastRecoveryTime;
                    if (diff < reuseAfterRecoveryMillis) {
                        reuseFailureReason = reject(id,
                                "was recovered recently and is not configured for reuse until another " + diff + "ms");
                        continue;
                    }
                }
            }

            // create a candidate. those with matching machine and instance id
            // are preferred, then the one with the lowest clusterId.
            candidates.add(new ClusterNodeInfo(id, store, mId, iId, false, invisible));
            startTimes.put(id, doc.getStartTime());
        }

        if (candidates.isEmpty()) {
            // No usable existing entry found
            int clusterNodeId;
            if (configuredClusterId != 0) {
                if (alreadyExistingConfigured != null) {
                    throw new DocumentStoreException(
                            "Configured cluster node id " + configuredClusterId + " already in use: " + reuseFailureReason);
                }
                clusterNodeId = configuredClusterId;
            } else {
                clusterNodeId = maxId + 1;
            }
            // No usable existing entry found so create a new entry
            candidates.add(new ClusterNodeInfo(clusterNodeId, store, machineId, instanceId, true, invisible));
        }

        // use the best candidate
        ClusterNodeInfo info = candidates.first();
        // and replace with an info matching the current machine and instance id
        info = new ClusterNodeInfo(info.id, store, machineId, instanceId, info.newEntry, invisible);
        return new AbstractMap.SimpleImmutableEntry<>(info, startTimes.get(info.getId()));
    }

    private static void logClusterIdAcquired(ClusterNodeInfo clusterNode,
                                             ClusterNodeInfoDocument before) {
        String type = clusterNode.newEntry ? "new" : "existing";
        type = clusterNode.invisible ? (type + " (invisible)") : type;

        String machineInfo = clusterNode.machineId;
        String instanceInfo = clusterNode.instanceId;
        String runtimeInfo = clusterNode.runtimeId;
        if (before != null) {
            // machineId or instanceId may have changed
            String beforeMachineId = String.valueOf(before.get(MACHINE_ID_KEY));
            String beforeInstanceId = String.valueOf(before.get(INSTANCE_ID_KEY));
            String beforeRuntimeId = String.valueOf(before.get(RUNTIME_ID_KEY));
            if (!clusterNode.machineId.equals(beforeMachineId)) {
                machineInfo = "(changed) " + beforeMachineId + " -> " + machineInfo;
            }
            if (!clusterNode.instanceId.equals(beforeInstanceId)) {
                instanceInfo = "(changed) " + beforeInstanceId + " -> " + instanceInfo;
            }
            if (!clusterNode.runtimeId.equals(beforeRuntimeId)) {
                runtimeInfo = "(changed) " + beforeRuntimeId + " -> " + runtimeInfo;
            }
        }
        LOG.info("Acquired ({}) clusterId {}. MachineId {}, InstanceId {}, RuntimeId {}",
                type, clusterNode.getId(), machineInfo, instanceInfo, runtimeInfo);

    }

    private static String reject(int id, String reason) {
        LOG.debug("Cannot acquire {}: {}", id, reason);
        return reason;
    }

    private static boolean waitForLeaseExpiry(DocumentStore store, ClusterNodeInfoDocument cdoc, long leaseEnd, String machineId,
            String instanceId) {
        String key = cdoc.getId();
        LOG.info("Found an existing possibly active cluster node info (" + key + ") for this instance: " + machineId + "/"
                + instanceId + ", will try use it.");

        // wait until lease expiry plus 2s
        long waitUntil = leaseEnd + 2000;

        while (getCurrentTime() < waitUntil) {
            LOG.info("Waiting for cluster node " + key + "'s lease to expire: " + (waitUntil - getCurrentTime()) / 1000 + "s left");

            try {
                clock.waitUntil(getCurrentTime() + 5000);
            } catch (InterruptedException e) {
                // ignored
            }

            try {
                // check state of cluster node info
                ClusterNodeInfoDocument reread = store.find(Collection.CLUSTER_NODES, key);
                if (reread == null) {
                    LOG.info("Cluster node info " + key + ": gone; continuing.");
                    return true;
                } else {
                    Long newLeaseEnd = (Long) reread.get(LEASE_END_KEY);
                    if (newLeaseEnd == null) {
                        LOG.info("Cluster node " + key + ": lease end information missing, aborting.");
                        return false;
                    } else {
                        if (newLeaseEnd != leaseEnd) {
                            LOG.info("Cluster node " + key + " seems to be still active (lease end changed from " + leaseEnd
                                    + " to " + newLeaseEnd + ", will not try to use it.");
                            return false;
                        }
                    }
                }
            } catch (DocumentStoreException ex) {
                LOG.info("Error reading cluster node info for key " + key, ex);
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if the lease for this cluster node is still valid, otherwise
     * throws a {@link DocumentStoreException}. Depending on the
     * {@link LeaseCheckMode} this method will not throw the
     * exception immediately when the lease expires. If the mode is set to
     * {@link LeaseCheckMode#LENIENT}, then this method will give the lease
     * update thread a last chance of 5 seconds to renew it. This allows the
     * DocumentNodeStore to recover from an expired lease caused by a system
     * put to sleep or a JVM in debug mode.
     *
     * @throws DocumentStoreException if the lease expired.
     */
    public void performLeaseCheck() throws DocumentStoreException {
        if (leaseCheckMode == LeaseCheckMode.DISABLED) {
            // if leaseCheckDisabled is set we never do the check, so return fast
            return;
        }
        if (leaseCheckFailed) {
            // unsynchronized access to leaseCheckFailed is fine
            // since it only ever changes from false to true once
            // and should the current thread read it erroneously
            // as false here, it would further down find out that
            // the lease has indeed still expired and then
            // go into the synchronized.
            // (note that once a lease check failed it would not
            // be updated again, ever, as guaranteed by checking
            // for leaseCheckFailed in renewLease() )
            throw leaseExpired(LEASE_CHECK_FAILED_MSG + " (since " + asISO8601(leaseEndTime) + ")", true);
        }
        long now = getCurrentTime();
        // OAK-3238 put the barrier 1/3 of 60sec=20sec before the end
        // OAK-3398 keeps this the same but uses an explicit leaseFailureMargin for this
        if (!isLeaseExpired(now)) {
            // then all is good
            return;
        }
        // synchronized: we need to guard leaseCheckFailed in order to ensure
        //               that it is only set by 1 thread - thus handleLeaseFailure
        //               is guaranteed to be only called once
        synchronized(this) {
            if (leaseCheckFailed) {
                // someone else won and marked leaseCheckFailed - so we only log/throw
                throw leaseExpired(LEASE_CHECK_FAILED_MSG + " (since " + asISO8601(leaseEndTime) + ")", true);
            }
            // only retry in lenient mode, fail immediately in strict mode
            final int maxRetries = leaseCheckMode == LeaseCheckMode.STRICT ?
                    0 : MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE;
            for (int i = 0; i < maxRetries; i++) {
                now = getCurrentTime();
                if (!isLeaseExpired(now)) {
                    // if lease is OK here, then there was a race
                    // between performLeaseCheck and renewLease()
                    // where the winner was: renewLease().
                    // so: luckily we can continue here
                    return;
                }
                // OAK-3399 : in case of running into the leaseFailureMargin
                // (shortly, 20sec, before the lease times out), we're now doing
                // a short retry loop of 1sec sleeps (default 5x1sec=5sec),
                // to give this instance 'one last chance' before we have to
                // declare the lease as failed.
                // This sort of retry loop would allow situations such as
                // when running a single-node cluster and interrupting/pausing
                // the process temporarily: in this case when waking up, the
                // lease might momentarily be timed out, but the lease would
                // still be 'updateable' and that would happen pretty soon
                // after waking up. So in that case, doing these retry-sleeps
                // would help.
                // in most other cases where the local instance is not doing
                // lease updates due to 'GC-death' or 'lease-thread-crashed'
                // or the like, it would not help. But it would also not hurt
                // as the margin is 20sec and we're just reducing it by 5sec
                // (in the un-paused case)
                try {
                    long difference = leaseEndTime - now;
                    long waitForMs = 1000;

                    String detail = difference >= 0
                            ? String.format("lease within %dms of failing (%dms precisely)", leaseFailureMargin, difference)
                            : String.format("already past lease end (%dms precisely)", -1 * difference);
                    String retries = String.format("waiting %dms to retry (up to another %d times...)",
                            waitForMs, maxRetries - 1 - i);
                    LOG.info("performLeaseCheck: " + detail + " - " + retries);

                    // directly use this to sleep on - to allow renewLease() to work
                    wait(waitForMs);
                } catch (InterruptedException e) {
                    LOG.warn("performLeaseCheck: got interrupted - giving up: " + e, e);
                    break;
                }
            }
            if (leaseCheckFailed) {
                // someone else won and marked leaseCheckFailed - so we only log/throw
                throw leaseExpired(LEASE_CHECK_FAILED_MSG + " (since " + asISO8601(leaseEndTime) + ")", true);
            }
            leaseCheckFailed = true; // make sure only one thread 'wins', ie goes any further
        }

        String template = "%s (mode: %s, leaseEndTime: %d (%s), leaseTime: %d, leaseFailureMargin: %d, "
                + "lease check end time (leaseEndTime - leaseFailureMargin): %d (%s), now: %d (%s), remaining: %d)"
                + " Need to stop oak-store-document/DocumentNodeStoreService.";
        String errorMsg = String.format(template, LEASE_CHECK_FAILED_MSG, leaseCheckMode.name(), leaseEndTime,
                asISO8601(leaseEndTime), leaseTime, leaseFailureMargin, leaseEndTime - leaseFailureMargin,
                asISO8601(leaseEndTime - leaseFailureMargin), now, asISO8601(now), (leaseEndTime - leaseFailureMargin) - now);

        LOG.error(errorMsg);
        handleLeaseFailure(errorMsg);
    }

    /**
     * Returns {@code true} if the lease for this cluster node info should be
     * considered expired given the current {@code time}. This method takes
     * {@link #leaseFailureMargin} into account and will return {@code true}
     * even before the passed {@code time} is beyond the {@link #leaseEndTime}.
     *
     * @param time the current time to check against the lease end.
     * @return {@code true} if the lease is considered expired, {@code false}
     *         otherwise.
     */
    protected boolean isLeaseExpired(long time) {
        return time >= (leaseEndTime - leaseFailureMargin);
    }

    private void handleLeaseFailure(final String errorMsg) {
        // OAK-3397 : unlike previously, when the lease check fails we should not
        // do a hard System exit here but rather stop the oak-store-document bundle
        // (or if that fails just deactivate DocumentNodeStore) - with the
        // goals to prevent this instance to continue to operate
        // give that a lease failure is a strong indicator of a faulty
        // instance - and to stop the background threads of DocumentNodeStore,
        // specifically the BackgroundLeaseUpdate and the BackgroundOperation.

        // actual stopping should be done in a separate thread, so:
        if (leaseFailureHandler!=null) {
            final Runnable r = new Runnable() {

                @Override
                public void run() {
                    if (leaseFailureHandler!=null) {
                        leaseFailureHandler.handleLeaseFailure();
                    }
                }
            };
            final Thread th = new Thread(r, "LeaseFailureHandler-Thread");
            th.setDaemon(true);
            th.start();
        }

        throw leaseExpired(errorMsg, false);
    }

    /**
     * Renew the cluster id lease. This method needs to be called once in a while,
     * to ensure the same cluster id is not re-used by a different instance.
     * The lease is only renewed after 'leaseUpdateInterval' millis
     * since last lease update - default being every 10 sec (this used to be 30sec).
     * <p>
     * This method will not fail immediately with a DocumentStoreException if
     * the lease expired. It will still try to renew the lease and only fail if
     * {@link #performLeaseCheck()} decided the lease expired or another cluster
     * node initiated recover for this node.
     *
     * @return {@code true} if the lease was renewed; {@code false} otherwise.
     * @throws DocumentStoreException if the operation failed or the lease
     *          expired.
     */
    public boolean renewLease() throws DocumentStoreException {
        long now = getCurrentTime();

        if (LOG.isTraceEnabled()) {
            LOG.trace("renewLease - leaseEndTime: " + leaseEndTime + ", leaseTime: " + leaseTime + ", leaseUpdateInterval: " + leaseUpdateInterval);
        }

        if (now < leaseEndTime - leaseTime + leaseUpdateInterval) {
            // no need to renew the lease - it is still within 'leaseUpdateInterval'
            return false;
        }
        // lease requires renewal

        long updatedLeaseEndTime;
        synchronized(this) {
            // this is synchronized since access to leaseCheckFailed and leaseEndTime
            // are both normally synchronized to propagate values between renewLease()
            // and performLeaseCheck().
            // (there are unsynchronized accesses to both of these as well - however
            // they are both double-checked - and with both reading a stale value is thus OK)

            if (leaseCheckFailed) {
                // prevent lease renewal after it failed
                throw leaseExpired(LEASE_CHECK_FAILED_MSG + " (since " + asISO8601(leaseEndTime) + ")", true);
            }
            // synchronized could have delayed the 'now', so
            // set it again..
            now = getCurrentTime();
            updatedLeaseEndTime = now + leaseTime;
        }

        if (leaseCheckMode == LeaseCheckMode.STRICT) {
            // check whether the lease is still valid and can be renewed
            if (isLeaseExpired(now)) {
                synchronized (this) {
                    if (leaseCheckFailed) {
                        // some other thread already noticed and calls failure handler
                        throw leaseExpired(LEASE_CHECK_FAILED_MSG + " (since " + asISO8601(leaseEndTime) + ")", true);
                    }
                    // current thread calls failure handler
                    // outside synchronized block
                    leaseCheckFailed = true;
                }
                String template = "%s (mode: %s, leaseEndTime: %d (%s), leaseTime: %d, leaseFailureMargin: %d, "
                        + "lease check end time (leaseEndTime - leaseFailureMargin): %d (%s), now: %d (%s), remaining: %d)"
                        + " Need to stop oak-store-document/DocumentNodeStoreService.";
                String errorMsg = String.format(template, LEASE_CHECK_FAILED_MSG, leaseCheckMode.name(), leaseEndTime,
                        asISO8601(leaseEndTime), leaseTime, leaseFailureMargin, leaseEndTime - leaseFailureMargin,
                        asISO8601(leaseEndTime - leaseFailureMargin), now, asISO8601(now), (leaseEndTime - leaseFailureMargin) - now);
                LOG.error(errorMsg);
                handleLeaseFailure(errorMsg);
                // should never be reached: handleLeaseFailure throws a DocumentStoreException
                return false;
            }
        }

        UpdateOp update = new UpdateOp("" + id, false);
        // Update the field only if the newer value is higher (or doesn't exist)
        update.max(LEASE_END_KEY, updatedLeaseEndTime);

        if (leaseCheckMode != LeaseCheckMode.DISABLED) {
            // if leaseCheckDisabled, then we just update the lease without
            // checking
            // OAK-3398:
            // if we renewed the lease ever with this instance/ClusterNodeInfo
            // (which is the normal case.. except for startup),
            // then we can now make an assertion that the lease is unchanged
            // and the incremental update must only succeed if no-one else
            // did a recover/inactivation in the meantime
            // make three assertions: it must still be active
            update.equals(STATE, null, ACTIVE.name());
            // plus it must not have a recovery lock on it
            update.notEquals(REV_RECOVERY_LOCK, ACQUIRED.name());
            // and the runtimeId that we create at startup each time
            // should be the same
            update.equals(RUNTIME_ID_KEY, null, runtimeId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Renewing lease for cluster id " + id + " with UpdateOp " + update);
        }
        Stopwatch sw = Stopwatch.createStarted();
        DocumentStoreException dse;
        Object result = null;
        try {
            ClusterNodeInfoDocument doc = store.findAndUpdate(Collection.CLUSTER_NODES, update);
            result = doc;

            if (doc == null) { // should not occur when leaseCheckDisabled
                // OAK-3398 : someone else either started recovering or is already through with that.
                // in both cases the local instance lost the lease-update-game - and hence
                // should behave and must consider itself as 'lease failed'

                synchronized(this) {
                    if (leaseCheckFailed) {
                        // somehow the instance figured out otherwise that the
                        // lease check failed - so we don't have to too - so we just log/throw
                        throw leaseExpired(LEASE_CHECK_FAILED_MSG, true);
                    }
                    leaseCheckFailed = true; // make sure only one thread 'wins', ie goes any further
                }

                String errorMsg = LEASE_CHECK_FAILED_MSG
                        + " (Could not update lease anymore, someone else in the cluster "
                        + "must have noticed this instance' slowness already. "
                        + "Going to invoke leaseFailureHandler!)";

                // try to add more diagnostics
                try {
                    ClusterNodeInfoDocument current = store.find(Collection.CLUSTER_NODES, "" + id);
                    if (current != null) {
                        Object leaseEnd = current.get(LEASE_END_KEY);
                        Object recoveryLock = current.get(REV_RECOVERY_LOCK);
                        Object recoveryBy = current.get(REV_RECOVERY_BY);
                        Object cnState = current.get(STATE);
                        errorMsg += " (leaseEnd: " + leaseEnd + " (expected: " + leaseEndTime + ")" +
                                ", (state: " + cnState + " (expected: " + ACTIVE.name() + ")" +
                                ", recoveryLock: " + recoveryLock +
                                ", recoveryBy: " + recoveryBy + ")";
                    }
                } catch (DocumentStoreException ex) {
                    LOG.error("trying to read ClusterNodeInfo for cluster id " + id, ex);
                }

                LOG.error(errorMsg);

                handleLeaseFailure(errorMsg);
                // should never be reached: handleLeaseFailure throws a DocumentStoreException
                return false;
            }
            leaseEndTime = updatedLeaseEndTime;
            String mode = (String) doc.get(READ_WRITE_MODE_KEY);
            if (mode != null && !mode.equals(readWriteMode)) {
                readWriteMode = mode;
                store.setReadWriteMode(mode);
            }
            return true;
        } catch (DocumentStoreException e) {
            dse = e;
            result = e.toString();
        } finally {
            sw.stop();
            String msg = "Lease renewal for cluster id {} took {}, resulted in: {}";
            if (sw.elapsed(TimeUnit.SECONDS) > 10) {
                LOG.warn(msg, id, sw, result);
            } else if (sw.elapsed(TimeUnit.SECONDS) > 1) {
                LOG.info(msg, id, sw, result);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(msg, id, sw, result);
            }
        }
        // if we get here, the update failed with an exception, try to read the
        // current cluster node info document and update leaseEndTime
        // accordingly until leaseEndTime is reached
        while (getCurrentTime() < updatedLeaseEndTime) {
            synchronized (this) {
                if (leaseCheckFailed) {
                    // no need to read from store, lease check already failed
                    break;
                }
            }
            long t1 = clock.getTime();
            ClusterNodeInfoDocument doc;
            try {
                doc = store.find(Collection.CLUSTER_NODES, String.valueOf(id));
            } catch (DocumentStoreException e) {
                LOG.warn("Reading ClusterNodeInfoDocument for id " + id + " failed", e);
                // do not retry more than once a second
                try {
                    clock.waitUntil(t1 + 1000);
                } catch (InterruptedException iex) {
                    // ignore
                }
                continue;
            }
            if (doc != null) {
                if (!doc.isActive()) {
                    LOG.warn("ClusterNodeInfoDocument for id {} is not active " +
                            "anymore. {}", id, doc);
                    // break here and let the next lease update attempt fail
                    break;
                } else if (runtimeId.equals(doc.getRuntimeId())) {
                    // set lease end time to current value, as they belong
                    // to this same cluster node
                    leaseEndTime = doc.getLeaseEndTime();
                    break;
                } else {
                    // leaseEndTime is neither the previous nor the new value
                    // another cluster node must have updated the leaseEndTime
                    // break here and let the next lease update attempt fail
                    break;
                }
            } else {
                LOG.warn("ClusterNodeInfoDocument for id {} does not exist anymore", id);
                break;
            }
        }
        throw dse;
    }
    
    /**
     * Update the cluster node info.
     * 
     * @param info the map of changes
     */
    public void setInfo(Map<String, String> info) {
        // synchronized, because renewLease is also synchronized 
        synchronized(this) {
            UpdateOp update = new UpdateOp("" + id, false);
            for(Entry<String, String> e : info.entrySet()) {
                update.set(e.getKey(), e.getValue());
            }
            store.findAndUpdate(Collection.CLUSTER_NODES, update);
        }
    }

    /** for testing purpose only, not to be changed at runtime! */
    void setLeaseTime(long leaseTime) {
        this.leaseTime = leaseTime;
    }

    /** for testing purpose only, not to be changed at runtime! */
    void setLeaseUpdateInterval(long leaseUpdateInterval) {
        this.leaseUpdateInterval = leaseUpdateInterval;
    }

    public long getLeaseTime() {
        return leaseTime;
    }

    public long getLeaseEndTime() {
        return leaseEndTime;
    }

    LeaseFailureHandler getLeaseFailureHandler() {
        return leaseFailureHandler;
    }

    public void setLeaseFailureHandler(LeaseFailureHandler leaseFailureHandler) {
        this.leaseFailureHandler = leaseFailureHandler;
    }

    public void dispose() {
        synchronized(this) {
            if (leaseCheckFailed) {
                LOG.warn("dispose: lease check failed, thus not marking instance as cleanly shut down.");
                return;
            }
        }
        UpdateOp update = new UpdateOp("" + id, true);
        update.set(LEASE_END_KEY, null);
        update.set(RECOVERY_TIME_KEY, null);
        update.set(STATE, null);
        update.set(INVISIBLE, false);
        update.set(RUNTIME_ID_KEY, null);
        store.createOrUpdate(Collection.CLUSTER_NODES, update);
        state = NONE;
    }

    @Override
    public String toString() {
        return "id: " + id + ",\n" +
                "startTime: " + startTime + ",\n" +
                "machineId: " + machineId + ",\n" +
                "instanceId: " + instanceId + ",\n" +
                "runtimeId: " + runtimeId + ",\n" +
                "pid: " + PROCESS_ID + ",\n" +
                "uuid: " + uuid + ",\n" +
                "readWriteMode: " + readWriteMode + ",\n" +
                "leaseCheckMode: " + leaseCheckMode.name() + ",\n" +
                "state: " + state + ",\n" +
                "oakVersion: " + OAK_VERSION + ",\n" +
                "formatVersion: " + DocumentNodeStore.VERSION + ",\n" +
                "invisible: " + invisible;
    }

    /**
     * Specify a custom clock to be used for determining current time.
     *
     * <b>Only Used For Testing</b>
     */
    static void setClock(Clock c) {
        requireNonNull(c);
        clock = c;
    }

    /**
     * Resets the clock to the default
     */
    static void resetClockToDefault() {
        clock = Clock.SIMPLE;
    }

    static long getRecoveryDelayMillis() {
        return recoveryDelayMillis;
    }

    static void setRecoveryDelayMillis(long recoveryDelayMillis) {
        ClusterNodeInfo.recoveryDelayMillis = recoveryDelayMillis;
    }

    /** <b>only used for testing</b> **/
    static void resetRecoveryDelayMillisToDefault() {
        recoveryDelayMillis = DEFAULT_RECOVERY_DELAY_MILLIS;
    }

    private static long getProcessId() {
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            return Long.parseLong(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            LOG.warn("Could not get process id", e);
            return 0;
        }
    }

    /*
     * Allow external override of hardware address. The special value "(none)"
     * indicates that a situation where no hardware address is available is to
     * be simulated.
     */
    private static String getHWAFromSystemProperty() {
        return SystemPropertySupplier.create(ClusterNodeInfo.class.getName() + ".HWADDRESS", "").loggingTo(LOG)
                .logSuccessAs("DEBUG")
                .formatSetMessage(
                        (name, value) -> String.format("obtaining hardware address from system variable %s: %s", name, value))
                .get();
    }

    /**
     * Calculate the unique machine id. This usually is the lowest MAC address
     * if available. As an alternative, a randomly generated UUID is used.
     *
     * @return the unique id
     */
    private static String getHardwareMachineId() {
        Exception exception = null;
        try {
            ArrayList<String> macAddresses = new ArrayList<String>();
            ArrayList<String> likelyVirtualMacAddresses = new ArrayList<String>();
            ArrayList<String> otherAddresses = new ArrayList<String>();
            String hwaFromSysProp = getHWAFromSystemProperty();
            if ("".equals(hwaFromSysProp)) {
                Enumeration<NetworkInterface> e = NetworkInterface
                        .getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface ni = e.nextElement();
                    try {
                        byte[] hwa = ni.getHardwareAddress();
                        // empty addresses have been seen on loopback devices
                        if (hwa != null && hwa.length != 0) {
                            String str = StringUtils.convertBytesToHex(hwa);
                            if (hwa.length == 6) {
                                // likely a MAC address
                                String displayName = ni.getDisplayName().toLowerCase(Locale.ENGLISH);
                                // de-prioritize addresses that are likely to be virtual (see OAK-3885 and OAK-8935)
                                boolean looksVirtual = displayName.indexOf("virtual") >= 0 || displayName.indexOf("vpn") >= 0
                                        || displayName.indexOf("docker") >= 0;
                                if (!looksVirtual) {
                                    macAddresses.add(str);
                                } else {
                                    likelyVirtualMacAddresses.add(str);
                                }
                            } else {
                                otherAddresses.add(str);
                            }
                        }
                    } catch (Exception e2) {
                        exception = e2;
                    }
                }
            }
            else {
                if (!"(none)".equals(hwaFromSysProp)) {
                    if (hwaFromSysProp.length() == 12) {
                        // assume 12 hex digits are a mac address
                        macAddresses.add(hwaFromSysProp);
                    } else {
                        otherAddresses.add(hwaFromSysProp);
                    }
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("getMachineId(): discovered addresses: {} {} {}", macAddresses, likelyVirtualMacAddresses,
                        otherAddresses);
            }

            if (macAddresses.size() > 0) {
                // use the lowest MAC value, such that if the order changes,
                // the same one is used
                Collections.sort(macAddresses);
                return "mac:" + macAddresses.get(0);
            } else if (likelyVirtualMacAddresses.size() > 0) {
                Collections.sort(likelyVirtualMacAddresses);
                return "mac:" + likelyVirtualMacAddresses.get(0);
            } else if (otherAddresses.size() > 0) {
                // try the lowest "other" address
                Collections.sort(otherAddresses);
                return "hwa:" + otherAddresses.get(0);
            }
        } catch (Exception e) {
            exception = e;
        }
        if (exception != null) {
            LOG.warn("Error getting the machine id; using a UUID", exception);
        }
        return RANDOM_PREFIX + UUID.randomUUID().toString();
    }

    private static long getCurrentTime() {
        return clock.getTime();
    }

    private static DocumentStoreException leaseExpired(String msg, boolean log) {
        if (log) {
            LOG.error(msg);
        }
        return new DocumentStoreException(msg);
    }
}
