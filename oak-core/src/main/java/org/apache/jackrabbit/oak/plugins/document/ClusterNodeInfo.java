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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.OakVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information about a cluster node.
 */
public class ClusterNodeInfo {

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
    private static final String OAK_VERSION_KEY = "oakVersion";

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
     * Additional info, such as the process id, for support.
     */
    private static final String INFO_KEY = "info";

    /**
     * The read/write mode key. Specifies the read/write preference to be used with
     * DocumentStore
     */
    private static final String READ_WRITE_MODE_KEY = "readWriteMode";

    /**
     * The unique machine id (the MAC address if available).
     */
    private static final String MACHINE_ID = getMachineId();

    /**
     * The process id (if available).
     */
    private static final long PROCESS_ID = getProcessId();

    /**
     * The current working directory.
     */
    private static final String WORKING_DIR = System.getProperty("user.dir", "");

    /**
     * <b>Only Used For Testing</b>
     */
    private static Clock clock = Clock.SIMPLE;


    public static final int DEFAULT_LEASE_DURATION_MILLIS = 1000 * 60;

    /**
     * The number of milliseconds for a lease (1 minute by default, and
     * initially).
     */
    private long leaseTime = DEFAULT_LEASE_DURATION_MILLIS;

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
     * The state of the cluter node.
     */
    private ClusterNodeState state;
    
    /**
     * Whether or not the OAK-2739/leaseCheck failed and thus a System.exit was already triggered
     * (is used to avoid calling System.exit a hundred times when it then happens)
     */
    private volatile boolean systemExitTriggered;

    /**
     * OAK-2739: for development it would be useful to be able to disable the
     * lease check - hence there's a system property that does that:
     * oak.documentMK.disableLeaseCheck
     */
    private final boolean leaseCheckDisabled;

    /**
     * Tracks the fact whether the lease has *ever* been renewed by this instance
     * or has just be read from the document store at initialization time.
     */
    private boolean renewed;

    /**
     * The revLock value of the cluster;
     */
    private RecoverLockState revRecoveryLock;

    /**
     * In memory flag indicating that this ClusterNode is entry is new and is being added to
     * DocumentStore for the first time
     *
     * If false then it indicates that a previous entry for current node existed and that is being
     * reused
     */
    private boolean newEntry;

    private ClusterNodeInfo(int id, DocumentStore store, String machineId, String instanceId, ClusterNodeState state,
            RecoverLockState revRecoveryLock, Long leaseEnd, boolean newEntry) {
        this.id = id;
        this.startTime = getCurrentTime();
        if (leaseEnd == null) {
            this.leaseEndTime = startTime;
        } else {
            this.leaseEndTime = leaseEnd;
        }
        this.renewed = false; // will be updated once we renew it the first time
        this.store = store;
        this.machineId = machineId;
        this.instanceId = instanceId;
        this.state = state;
        this.revRecoveryLock = revRecoveryLock;
        this.newEntry = newEntry;
        this.leaseCheckDisabled = Boolean.valueOf(System.getProperty("oak.documentMK.disableLeaseCheck", "false"));
    }

    public int getId() {
        return id;
    }

    /**
     * Create a cluster node info instance for the store, with the
     * 
     * @param store the document store (for the lease)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store) {
        return getInstance(store, MACHINE_ID, WORKING_DIR, false);
    }

    /**
     * Create a cluster node info instance for the store.
     * 
     * @param store the document store (for the lease)
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store, String machineId,
            String instanceId) {
        return getInstance(store, machineId, instanceId, true);
    }

    /**
     * Create a cluster node info instance for the store.
     *
     * @param store the document store (for the lease)
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @param updateLease whether to update the lease
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store, String machineId,
            String instanceId, boolean updateLease) {
        if (machineId == null) {
            machineId = MACHINE_ID;
        }
        if (instanceId == null) {
            instanceId = WORKING_DIR;
        }
        for (int i = 0; i < 10; i++) {
            ClusterNodeInfo clusterNode = createInstance(store, machineId, instanceId);
            UpdateOp update = new UpdateOp("" + clusterNode.id, true);
            update.set(ID, String.valueOf(clusterNode.id));
            update.set(MACHINE_ID_KEY, clusterNode.machineId);
            update.set(INSTANCE_ID_KEY, clusterNode.instanceId);
            if (updateLease) {
                update.set(LEASE_END_KEY, getCurrentTime() + clusterNode.leaseTime);
            } else {
                update.set(LEASE_END_KEY, clusterNode.leaseEndTime);
            }
            update.set(INFO_KEY, clusterNode.toString());
            update.set(STATE, clusterNode.state.name());
            update.set(REV_RECOVERY_LOCK, clusterNode.revRecoveryLock.name());
            update.set(OAK_VERSION_KEY, OakVersion.getVersion());

            final boolean success;
            if (clusterNode.newEntry) {
                //For new entry do a create. This ensures that if two nodes create
                //entry with same id then only one would succeed
                success = store.create(Collection.CLUSTER_NODES, Collections.singletonList(update));
            } else {
                // No expiration of earlier cluster info, so update
                store.createOrUpdate(Collection.CLUSTER_NODES, update);
                success = true;
            }

            if (success) {
                return clusterNode;
            }
        }
        throw new DocumentStoreException("Could not get cluster node info");
    }

    private static ClusterNodeInfo createInstance(DocumentStore store, String machineId,
            String instanceId) {
        long now = getCurrentTime();
        List<ClusterNodeInfoDocument> list = ClusterNodeInfoDocument.all(store);
        int clusterNodeId = 0;
        int maxId = 0;
        ClusterNodeState state = ClusterNodeState.NONE;
        Long prevLeaseEnd = null;
        boolean newEntry = false;
        for (Document doc : list) {
            String key = doc.getId();
            int id;
            try {
                id = Integer.parseInt(key);
            } catch (Exception e) {
                // not an integer - ignore
                continue;
            }
            maxId = Math.max(maxId, id);
            Long leaseEnd = (Long) doc.get(LEASE_END_KEY);
            if (leaseEnd != null && leaseEnd > now) {
                continue;
            }
            String mId = "" + doc.get(MACHINE_ID_KEY);
            String iId = "" + doc.get(INSTANCE_ID_KEY);
            if (machineId.startsWith(RANDOM_PREFIX)) {
                // remove expired entries with random keys
                store.remove(Collection.CLUSTER_NODES, key);
                continue;
            }
            if (!mId.equals(machineId) ||
                    !iId.equals(instanceId)) {
                // a different machine or instance
                continue;
            }

            //and cluster node which matches current machine identity but
            //not being used
            if (clusterNodeId == 0 || id < clusterNodeId) {
                // if there are multiple, use the smallest value
                clusterNodeId = id;
                state = ClusterNodeState.fromString((String) doc.get(STATE));
                prevLeaseEnd = leaseEnd;
            }
        }

        //No existing entry with matching signature found so
        //create a new entry
        if (clusterNodeId == 0) {
            clusterNodeId = maxId + 1;
            newEntry = true;
        }

        // Do not expire entries and stick on the earlier state, and leaseEnd so,
        // that _lastRev recovery if needed is done.        
        return new ClusterNodeInfo(clusterNodeId, store, machineId, instanceId, state, 
                RecoverLockState.NONE, prevLeaseEnd, newEntry);
    }

    public void performLeaseCheck() {
        if (leaseCheckDisabled || !renewed) {
            // if leaseCheckDisabled is set we never do the check, so return fast

            // the 'renewed' flag indicates if this instance *ever* renewed the lease after startup
            // until that is not set, we cannot do the lease check (otherwise startup wouldn't work)
            return;
        }
        final long now = getCurrentTime();
        if (now < (leaseEndTime - leaseTime / 3)) { // OAK-3238 : put the barrier 1/3 before lease end
            // then all is good
            return;
        }

        // OAK-2739 : when the lease is not current, we must stop
        // the instance immediately to avoid any cluster inconsistency
        final String errorMsg = "performLeaseCheck: this instance failed to update the lease in time "
                + "(leaseEndTime: "+leaseEndTime+", now: "+now+", leaseTime: "+leaseTime+") "
                + "and is thus no longer eligible for taking part in the cluster. Shutting down NOW!";
        LOG.error(errorMsg);

        // now here comes the thing: we should a) call System.exit in a separate thread
        // to avoid any deadlock when calling from eg within the shutdown hook
        // AND b) we should not call system.exit hundred times.
        // so for b) we use 'systemExitTriggered' to avoid calling it over and over
        // BUT it doesn't have to be 100% ensured that system.exit is called only once.
        // it is fine if it gets called once, twice - but just not hundred times.
        // which is a long way of saying: volatile is fine here - and the 'if' too
        if (!systemExitTriggered) {
            systemExitTriggered = true;
            final Runnable r = new Runnable() {

                @Override
                public void run() {
                    System.exit(-1);
                }
            };
            final Thread th = new Thread(r, "FailedLeaseCheckShutdown-Thread");
            th.setDaemon(true);
            th.start();
        }
        throw new AssertionError(errorMsg);
    }

    /**
     * Renew the cluster id lease. This method needs to be called once in a while,
     * to ensure the same cluster id is not re-used by a different instance.
     * The lease is only renewed when a third of the lease time passed. That is,
     * with a lease time of 60 seconds, the lease is renewed every 20 seconds.
     *
     * @return {@code true} if the lease was renewed; {@code false} otherwise.
     */
    public boolean renewLease() {
        long now = getCurrentTime();
        if (now + 2 * leaseTime / 3 < leaseEndTime) {
            return false;
        }
        UpdateOp update = new UpdateOp("" + id, true);
        leaseEndTime = now + leaseTime;
        update.set(LEASE_END_KEY, leaseEndTime);
        update.set(STATE, ClusterNodeState.ACTIVE.name());
        ClusterNodeInfoDocument doc = store.createOrUpdate(Collection.CLUSTER_NODES, update);
        String mode = (String) doc.get(READ_WRITE_MODE_KEY);
        if (mode != null && !mode.equals(readWriteMode)) {
            readWriteMode = mode;
            store.setReadWriteMode(mode);
        }
        renewed = true;
        return true;
    }

    public void setLeaseTime(long leaseTime) {
        this.leaseTime = leaseTime;
    }

    public long getLeaseTime() {
        return leaseTime;
    }

    public void dispose() {
        UpdateOp update = new UpdateOp("" + id, true);
        update.set(LEASE_END_KEY, null);
        update.set(STATE, null);
        update.set(REV_RECOVERY_LOCK, null);
        store.createOrUpdate(Collection.CLUSTER_NODES, update);
    }

    @Override
    public String toString() {
        return "id: " + id + ",\n" +
                "startTime: " + startTime + ",\n" +
                "machineId: " + machineId + ",\n" +
                "instanceId: " + instanceId + ",\n" +
                "pid: " + PROCESS_ID + ",\n" +
                "uuid: " + uuid + ",\n" +
                "readWriteMode: " + readWriteMode + ",\n" +
                "state: " + state + ",\n" +
                "revLock: " + revRecoveryLock;
    }

    /**
     * Specify a custom clock to be used for determining current time.
     *
     * <b>Only Used For Testing</b>
     */
    static void setClock(Clock c) {
        checkNotNull(c);
        clock = c;
    }

    /**
     * Resets the clock to the default
     */
    static void resetClockToDefault() {
        clock = Clock.SIMPLE;
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

    /**
     * Calculate the unique machine id. This is the lowest MAC address if
     * available. As an alternative, a randomly generated UUID is used.
     * 
     * @return the unique id
     */
    private static String getMachineId() {
        Exception exception = null;
        try {
            ArrayList<String> list = new ArrayList<String>();
            Enumeration<NetworkInterface> e = NetworkInterface
                    .getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface ni = e.nextElement();
                try {
                    byte[] mac = ni.getHardwareAddress();
                    if (mac != null) {
                        String x = StringUtils.convertBytesToHex(mac);
                        list.add(x);
                    }
                } catch (Exception e2) {
                    exception = e2;
                }
            }
            if (list.size() > 0) {
                // use the lowest value, such that if the order changes,
                // the same one is used
                Collections.sort(list);
                return "mac:" + list.get(0);
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

}
