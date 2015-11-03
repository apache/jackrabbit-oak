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
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.OakVersion;
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
     * The unique machine id (the MAC address if available).
     */
    private static final String MACHINE_ID = getMachineId();

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
    public static final int DEFAULT_LEASE_DURATION_MILLIS = 1000 * 120;

    /** OAK-3398 : default update interval 10sec **/
    public static final int DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS = 1000 * 10;

    /** OAK-3398 : default failure margin 20sec before actual lease timeout
     * (note that OAK-3399 / MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE eats
     * off another few seconds from this margin, by default 5sec,
     * so the actual default failure-margin is down to 15sec - and that is high-noon!)
     */
    public static final int DEFAULT_LEASE_FAILURE_MARGIN_MILLIS = 1000 * 20;

    /** OAK-3399 : max number of times we're doing a 1sec retry loop just before declaring lease failure **/
    private static final int MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE = 5;

    /**
     * The Oak version.
     */
    private static final String OAK_VERSION = OakVersion.getVersion();

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
     * The value of leaseEnd last updated towards DocumentStore -
     * this one is used to compare against (for OAK-3398) when checking
     * if any other instance updated the lease or if the lease is unchanged.
     * (This is kind of a duplication of the leaseEndTime field, yes - but the semantics
     * are that previousLeaseEndTime exactly only serves the purpose of
     * keeping the value of what was stored in the previous lease update.
     * leaseEndTime on the other hand serves the purpose of *defining the lease end*,
     * these are two different concerns, thus justify two different fields.
     * the leaseEndTime for example can be manipulated during tests therefore,
     * without interfering with previousLeaseEndTime)
     */
    private long previousLeaseEndTime;

    /**
     * The read/write mode.
     */
    private String readWriteMode;

    /**
     * The state of the cluster node.
     */
    private ClusterNodeState state;

    /**
     * OAK-2739 / OAK-3397 : once a lease check turns out negative, this flag
     * is set to prevent any further checks to succeed. Also, only the first
     * one to change this flag will take the appropriate action that results
     * from a failed leaseCheck (which is currently to stop oak-core bundle)
     */
    private boolean leaseCheckFailed = false;

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

    /** OAK-3397 / OAK-3400 : the LeaseFailureHandler is the one that actually stops the oak-core bundle (or does something else if necessary) **/
    private LeaseFailureHandler leaseFailureHandler;

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
     * @param configuredClusterId the configured cluster id (or 0 for dynamic assignment)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store, int configuredClusterId) {
        return getInstance(store, MACHINE_ID, WORKING_DIR, configuredClusterId, false);
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
        return getInstance(store, machineId, instanceId, 0, true);
    }

    /**
     * Create a cluster node info instance for the store.
     *
     * @param store the document store (for the lease)
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @param configuredClusterId the configured cluster id (or 0 for dynamic assignment)
     * @param updateLease whether to update the lease
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store, String machineId,
            String instanceId, int configuredClusterId, boolean updateLease) {

        // defaults for machineId and instanceID
        if (machineId == null) {
            machineId = MACHINE_ID;
        }
        if (instanceId == null) {
            instanceId = WORKING_DIR;
        }

        int retries = 10;
        for (int i = 0; i < retries; i++) {
            ClusterNodeInfo clusterNode = createInstance(store, machineId, instanceId, configuredClusterId);
            String key = String.valueOf(clusterNode.id);
            UpdateOp update = new UpdateOp(key, true);
            update.set(ID, key);
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
            update.set(OAK_VERSION_KEY, OAK_VERSION);

            final boolean success;
            if (clusterNode.newEntry) {
                // For new entry do a create. This ensures that if two nodes
                // create entry with same id then only one would succeed
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
        throw new DocumentStoreException("Could not get cluster node info (retried " + retries + " times)");
    }

    private static ClusterNodeInfo createInstance(DocumentStore store, String machineId,
            String instanceId, int configuredClusterId) {

        long now = getCurrentTime();
        int clusterNodeId = 0;
        int maxId = 0;
        ClusterNodeState state = ClusterNodeState.NONE;
        Long prevLeaseEnd = null;
        boolean newEntry = false;

        ClusterNodeInfoDocument alreadyExistingConfigured = null;
        String reuseFailureReason = "";
        List<ClusterNodeInfoDocument> list = ClusterNodeInfoDocument.all(store);

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

            if (leaseEnd != null && leaseEnd > now) {
                // TODO wait for lease end, see OAK-3449
                reuseFailureReason = "leaseEnd " + leaseEnd + " > " + now + " - " + (leaseEnd - now) + "ms in the future";
                continue;
            }

            String mId = "" + doc.get(MACHINE_ID_KEY);
            String iId = "" + doc.get(INSTANCE_ID_KEY);

            // remove entries with "random:" keys if not in use (no lease at all) 
            if (mId.startsWith(RANDOM_PREFIX) && leaseEnd == null) {
                store.remove(Collection.CLUSTER_NODES, key);
                LOG.debug("Cleaned up cluster node info for clusterNodeId {} [machineId: {}, leaseEnd: {}]", id, mId,
                        leaseEnd == null ? "n/a" : Utils.timestampToString(leaseEnd));
                if (alreadyExistingConfigured == doc) {
                    // we removed it, so we can't re-use it after all
                    alreadyExistingConfigured = null;
                }
                continue;
            }

            if (!mId.equals(machineId) || !iId.equals(instanceId)) {
                // a different machine or instance
                reuseFailureReason = "machineId/instanceId do not match: " + mId + "/" + iId + " != " + machineId + "/" + instanceId;
                continue;
            }

            // a cluster node which matches current machine identity but
            // not being used
            if (clusterNodeId == 0 || id < clusterNodeId) {
                // if there are multiple, use the smallest value
                clusterNodeId = id;
                state = ClusterNodeState.fromString((String) doc.get(STATE));
                prevLeaseEnd = leaseEnd;
            }
        }

        // No usable existing entry with matching signature found so
        // create a new entry
        if (clusterNodeId == 0) {
            newEntry = true;
            if (configuredClusterId != 0) {
                if (alreadyExistingConfigured != null) {
                    throw new DocumentStoreException(
                            "Configured cluster node id " + configuredClusterId + " already in use: " + reuseFailureReason);
                }
                clusterNodeId = configuredClusterId;
            } else {
                clusterNodeId = maxId + 1;
            }
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
            LOG.error(LEASE_CHECK_FAILED_MSG);
            throw new AssertionError(LEASE_CHECK_FAILED_MSG);
        }
        long now = getCurrentTime();
        // OAK-3238 put the barrier 1/3 of 60sec=20sec before the end
        // OAK-3398 keeps this the same but uses an explicit leaseFailureMargin for this
        if (now < (leaseEndTime - leaseFailureMargin)) {
            // then all is good
            return;
        }
        // synchronized: we need to guard leaseCheckFailed in order to ensure
        //               that it is only set by 1 thread - thus handleLeaseFailure
        //               is guaranteed to be only called once
        synchronized(this) {
            if (leaseCheckFailed) {
                // someone else won and marked leaseCheckFailed - so we only log/throw
                LOG.error(LEASE_CHECK_FAILED_MSG);
                throw new AssertionError(LEASE_CHECK_FAILED_MSG);
            }
            for(int i=0; i<MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE; i++) {
                now = getCurrentTime();
                if (now < (leaseEndTime - leaseFailureMargin)) {
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
                    LOG.info("performLeaseCheck: lease within "+leaseFailureMargin+
                            "ms of failing ("+(leaseEndTime-now)+" ms precisely) - "
                            + "waiting 1sec to retry (up to another "+
                            (MAX_RETRY_SLEEPS_BEFORE_LEASE_FAILURE-1-i)+" times)...");
                    wait(1000); // directly use this to sleep on - to allow renewLease() to work
                } catch (InterruptedException e) {
                    LOG.warn("performLeaseCheck: got interrupted - giving up: "+e, e);
                    break;
                }
            }
            leaseCheckFailed = true; // make sure only one thread 'wins', ie goes any further
        }

        final String errorMsg = LEASE_CHECK_FAILED_MSG+" (leaseEndTime: "+leaseEndTime+
                ", leaseTime: "+leaseTime+
                ", leaseFailureMargin: "+leaseFailureMargin+
                ", lease check end time (leaseEndTime-leaseFailureMargin): "+(leaseEndTime - leaseFailureMargin)+
                ", now: "+now+
                ", remaining: "+((leaseEndTime - leaseFailureMargin) - now)+
                ") Need to stop oak-core/DocumentNodeStoreService.";
        LOG.error(errorMsg);

        handleLeaseFailure(errorMsg);
    }

    private void handleLeaseFailure(final String errorMsg) {
        // OAK-3397 : unlike previously, when the lease check fails we should not
        // do a hard System exit here but rather stop the oak-core bundle
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

        throw new AssertionError(errorMsg);
    }

    /**
     * Renew the cluster id lease. This method needs to be called once in a while,
     * to ensure the same cluster id is not re-used by a different instance.
     * The lease is only renewed after 'leaseUpdateInterval' millis
     * since last lease update - default being every 10 sec (this used to be 30sec).
     *
     * @return {@code true} if the lease was renewed; {@code false} otherwise.
     */
    public boolean renewLease() {
        long now = getCurrentTime();
        if (now < leaseEndTime - leaseTime + leaseUpdateInterval) {
            // no need to renew the lease - it is still within 'leaseUpdateInterval'
            return false;
        }
        // lease requires renewal

        synchronized(this) {
            // this is synchronized since access to leaseCheckFailed and leaseEndTime
            // are both normally synchronized to propagate values between renewLease()
            // and performLeaseCheck().
            // (there are unsynchronized accesses to both of these as well - however
            // they are both double-checked - and with both reading a stale value is thus OK)

            if (leaseCheckFailed) {
                // prevent lease renewal after it failed
                LOG.error(LEASE_CHECK_FAILED_MSG);
                throw new AssertionError(LEASE_CHECK_FAILED_MSG);
            }
            // synchronized could have delayed the 'now', so
            // set it again..
            now = getCurrentTime();
            leaseEndTime = now + leaseTime;
        }
        UpdateOp update = new UpdateOp("" + id, false);
        update.set(LEASE_END_KEY, leaseEndTime);
        update.set(STATE, ClusterNodeState.ACTIVE.name());
        ClusterNodeInfoDocument doc = null;
        if (renewed && !leaseCheckDisabled) { // if leaseCheckDisabled, then we just update the lease without checking
            // OAK-3398:
            // if we renewed the lease ever with this instance/ClusterNodeInfo
            // (which is the normal case.. except for startup),
            // then we can now make an assertion that the lease is unchanged
            // and the incremental update must only succeed if no-one else
            // did a recover/inactivation in the meantime
            update.setNew(false); // in this case it is *not* a new document
            // make two assertions: the leaseEnd must match ..
            update.equals(LEASE_END_KEY, null, previousLeaseEndTime);
            // plus it must still be active ..
            update.equals(STATE, null, ClusterNodeState.ACTIVE.name());
            // @TODO: to make it 100% failure proof we could introduce
            // yet another field to clusterNodes: a runtimeId that we
            // create (UUID) at startup each time - and against that
            // we could also check here - but that goes a bit far IMO
            doc = store.findAndUpdate(Collection.CLUSTER_NODES, update);
        } else {
            // this is only for startup - then we 'just' overwrite
            // the lease - or create it - and don't care a lot about what the
            // status of the lease was
            doc = store.findAndUpdate(Collection.CLUSTER_NODES, update);
        }
        if (doc==null) { // should not occur when leaseCheckDisabled
            // OAK-3398 : someone else either started recovering or is already through with that.
            // in both cases the local instance lost the lease-update-game - and hence
            // should behave and must consider itself as 'lease failed'

            synchronized(this) {
                if (leaseCheckFailed) {
                    // somehow the instance figured out otherwise that the
                    // lease check failed - so we don't have to too - so we just log/throw
                    LOG.error(LEASE_CHECK_FAILED_MSG);
                    throw new AssertionError(LEASE_CHECK_FAILED_MSG);
                }
                leaseCheckFailed = true; // make sure only one thread 'wins', ie goes any further
            }
            final String errorMsg = LEASE_CHECK_FAILED_MSG+
                    " (Could not update lease anymore, someone else in the cluster "
                    + "must have noticed this instance' slowness already. "
                    + "Going to invoke leaseFailureHandler!)";
            LOG.error(errorMsg);

            handleLeaseFailure(errorMsg);
            // should never be reached: handleLeaseFailure throws an AssertionError
            return false;
        }
        previousLeaseEndTime = leaseEndTime; // store previousLeaseEndTime for reference for next time
        String mode = (String) doc.get(READ_WRITE_MODE_KEY);
        if (mode != null && !mode.equals(readWriteMode)) {
            readWriteMode = mode;
            store.setReadWriteMode(mode);
        }
        renewed = true;
        return true;
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
        update.set(STATE, null);
        update.set(REV_RECOVERY_LOCK, RecoverLockState.NONE.name());
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
                "revLock: " + revRecoveryLock + ",\n" +
                "oakVersion: " + OAK_VERSION;
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

    /*
     * Allow external override of hardware address. The special value "(none)"
     * indicates that a situation where no hardware address is available is to
     * be simulated.
     */
    private static String getHWAFromSystemProperty() {
        String pname = ClusterNodeInfo.class.getName() + ".HWADDRESS";
        String hwa = System.getProperty(pname, "");
        if (!"".equals(hwa)) {
            LOG.debug("obtaining hardware address from system variable " + pname + ": " + hwa);
        }
        return hwa;
    }

    /**
     * Calculate the unique machine id. This usually is the lowest MAC address
     * if available. As an alternative, a randomly generated UUID is used.
     *
     * @return the unique id
     */
    private static String getMachineId() {
        Exception exception = null;
        try {
            ArrayList<String> macAddresses = new ArrayList<String>();
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
                                macAddresses.add(str);
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
                LOG.debug("getMachineId(): discovered addresses: {} {}", macAddresses, otherAddresses);
            }

            if (macAddresses.size() > 0) {
                // use the lowest MAC value, such that if the order changes,
                // the same one is used
                Collections.sort(macAddresses);
                return "mac:" + macAddresses.get(0);
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
}
