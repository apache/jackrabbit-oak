/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Value;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.Version;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DocumentDiscoveryLiteService is taking care of providing a repository
 * descriptor that contains the current cluster-view details.
 * <p>
 * The clusterView is provided via a repository descriptor (see
 * OAK_DISCOVERYLITE_CLUSTERVIEW)
 * <p>
 * The cluster-view lists all instances (ever) known in the cluster in one of
 * the following states:
 * <ul>
 * <li>active: the instance is currently running and has an up-to-date lease
 * </li>
 * <li>deactivating: the instance failed to update the lease recently thus a
 * recovery is happening - or it has just finished and the local instance is yet
 * to do a backgroundRead before it has finished reading the crashed/shutdown
 * instance's last changes</li>
 * <li>inactive: the instance is currently not running and all its changes have
 * been seen by the local instance</li>
 * </ul>
 * <p>
 * Additionally, the cluster-view is assigned a monotonically increasing
 * sequence number to. This sequence number is persisted, thus all instances in
 * the cluster will show the same sequence number for a particular cluster-view
 * in time.
 * <p>
 * Note that the 'deactivating' state might be hiding some complexity that is
 * deliberately not shown: for the documentNS the state 'deactivating' consists
 * of two substates: 'recovering' as in _lastRevs are updated, and 'backlog
 * processing' for a pending backgroundRead to get the latest head state of a
 * crashed/shutdown instance. So when an instance is in 'deactivating' state, it
 * is not indicated via the cluster-view whether it is recovering or has backlog
 * to process. However, the fact that an instance has to yet do a backgroundRead
 * to get changes is a per-instance story: other instances might already have
 * done the backgroundRead and thus no longer have a backlog for the instance(s)
 * that left. Even though 'deactivating' therefore is dependent on the instance
 * you get the information from, the cluster-view must have a sequence number
 * that uniquely identifies it in the cluster. These two constraints conflict.
 * As a simple solution to handle this case nevertheless, the 'final' flag has
 * been introduced: the cluster-view has this flag 'final' set to true when the
 * view is final and nothing will be changed in this sequence number anymore. If
 * the 'final' flag is false however it indicates that the cluster-view with
 * this particular sequence number might still experience a change (more
 * concretely: the deactivating instances might change). Note that there
 * alternatives to this 'final' flag have been discussed, such as using
 * vector-counters, but there was no obvious gain achieve using an alternative
 * approach.
 * <p>
 * In other words: whenever the 'final' flag is false, the view must be
 * interpreted as 'in flux' wrt the deactivating/inactive instances and any
 * action that depends on stable deactivating/inactive instances must not yet be
 * done until the 'final' flag becomes true.
 * <p>
 * Underneath, the DocumentDiscoveryLiteService uses the clusterNodes collection
 * to derive the clusterView, which it stores in the settings collection.
 * Whenever it updates the clusterView it increments the sequence number by 1.
 * <p>
 * While this new 'clusterView' document in the settings collection sounds like
 * redundant data (since it is just derived from the clusterNodes), it actually
 * is not. By persisting the clusterView it becomes the new source of truth wrt
 * what the clusterView looks like. And no two instances in the same cluster can
 * make different conclusions based eg on different clocks they have or based on
 * reading the clusterNodes in a slightly different moment etc. Also, the
 * clusterView allows to store a the sequence number
 * (which allows the instances to make reference to the same clusterView, and be
 * able to simply detect whether anything has changed)
 * <p>
 * Prerequisites that the clusterView mechanism is stable:
 * <ul>
 * <li>the machine clocks are reasonably in sync - that is, they should be off
 * by magnitudes less than the lease updateFrequency/timeout</li>
 * <li>the write-delays from any instance to the mongo server where the
 * clusterNodes and settings collections are stored should be very fast - at
 * least orders of magnitudes lower again than the lease timeout</li>
 * <li>when this instance notices that others have kicked it out of the
 * clusterView (which it can find out when either its clusterNodes document is
 * set to recovering or it is not in the clusterView anymore, although it just
 * was - ie not just because of a fresh start), then this instance must step
 * back gracefully. The exact definition is to be applied elsewhere - but it
 * should include: stopping to update its own lease, waiting for the view to
 * have stabilized - waiting for recovery of its own instance by the remaining
 * instances in the cluster to have finished - and then probably waiting for
 * another gracePeriod until it might rejoin the cluster. In between, any commit
 * should fail with BannedFromClusterException</li>
 * </ul>
 * 
 * @see #OAK_DISCOVERYLITE_CLUSTERVIEW
 */
@Component(
        name = DocumentDiscoveryLiteService.COMPONENT_NAME,
        immediate = true,
        service = { DocumentDiscoveryLiteService.class, Observer.class })
public class DocumentDiscoveryLiteService implements ClusterStateChangeListener, Observer {

    static final String COMPONENT_NAME = "org.apache.jackrabbit.oak.plugins.document.DocumentDiscoveryLiteService";

    /**
     * Name of the repository descriptor via which the clusterView is published
     * - which is the raison d'etre of the DocumentDiscoveryLiteService
     **/
    public static final String OAK_DISCOVERYLITE_CLUSTERVIEW = "oak.discoverylite.clusterview";

    private static final Logger logger = LoggerFactory.getLogger(DocumentDiscoveryLiteService.class);

    /** describes the reason why the BackgroundWorker should be woken up **/
    private static enum WakeupReason {
        CLUSTER_STATE_CHANGED, BACKGROUND_READ_FINISHED
    }

    /**
     * The BackgroundWorker is taking care of regularly invoking checkView -
     * which in turn will detect if anything changed
     **/
    private class BackgroundWorker implements Runnable {

        final Random random = new Random();

        boolean stopped = false;

        private void stop() {
            logger.trace("stop: start");
            synchronized (BackgroundWorker.this) {
                stopped = true;
            }
            logger.trace("stop: end");
        }

        @Override
        public void run() {
            logger.info("BackgroundWorker.run: start");
            try {
                doRun();
            } finally {
                logger.info("BackgroundWorker.run: end {finally}");
            }
        }

        private void doRun() {
            while (!stopped) {
                try {
                    logger.trace("BackgroundWorker.doRun: going to call checkView");
                    boolean shortSleep = checkView();
                    logger.trace("BackgroundWorker.doRun: checkView terminated with {} (=shortSleep)", shortSleep);
                    long sleepMillis = shortSleep ? (50 + random.nextInt(450)) : 5000;
                    logger.trace("BackgroundWorker.doRun: sleeping {}ms", sleepMillis);
                    synchronized (BackgroundWorker.this) {
                        if (stopped)
                            return;
                        BackgroundWorker.this.wait(sleepMillis);
                        if (stopped)
                            return;
                    }
                    logger.trace("BackgorundWorker.doRun: done sleeping, looping");
                } catch (Exception e) {
                    logger.error("doRun: got an exception: " + e, e);
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e2) {
                        logger.error("doRun: got an exception while sleeping due to another exception: " + e2, e2);
                    }
                }
            }
        }

    }

    /** This provides the 'clusterView' repository descriptors **/
    private class DiscoveryLiteDescriptor implements Descriptors {

        final SimpleValueFactory factory = new SimpleValueFactory();

        @Override
        public String[] getKeys() {
            return new String[] { OAK_DISCOVERYLITE_CLUSTERVIEW };
        }

        @Override
        public boolean isStandardDescriptor(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return false;
            }
            return true;
        }

        @Override
        public boolean isSingleValueDescriptor(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return false;
            }
            return true;
        }

        @Override
        public Value getValue(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return null;
            }
            return factory.createValue(getClusterViewAsDescriptorValue());
        }

        @Override
        public Value[] getValues(String key) {
            if (!OAK_DISCOVERYLITE_CLUSTERVIEW.equals(key)) {
                return null;
            }
            return new Value[] { getValue(key) };
        }

    }

    /** DocumentNodeStore's (hence local) clusterId **/
    private int clusterNodeId = -1;

    /**
     * the DocumentNodeStore - used to get the active/inactive cluster ids from
     **/
    private DocumentNodeStore documentNodeStore;

    /**
     * background job that periodically verifies and updates the clusterView
     **/
    private BackgroundWorker backgroundWorker;

    /** the ClusterViewDocument which was used in the last checkView run **/
    private ClusterViewDocument previousClusterViewDocument;

    /**
     * the ClusterView that was valid as a result of the previous checkView run
     **/
    private ClusterView previousClusterView;

    /**
     * kept volatile as this is frequently read in contentChanged which is
     * better kept unsynchronized as long as possible
     **/
    private volatile boolean hasInstancesWithBacklog;

    /**
     * Require a static reference to the NodeStore. Note that this implies the
     * service is only active for documentNS
     **/
    @Reference
    private DocumentNodeStore nodeStore;

    /**
     * inactive nodes that have been so for a while, ie they have no backlog
     * anymore, so no need to check for backlog every time
     **/
    private Set<Integer> longTimeInactives = new HashSet<Integer>();

    /**
     * returns the clusterView as a json value for it to be provided via the
     * repository descriptor
     **/
    private String getClusterViewAsDescriptorValue() {
        if (previousClusterView == null) {
            return null;
        } else {
            return previousClusterView.asDescriptorValue();
        }
    }

    /**
     * On activate the DocumentDiscoveryLiteService tries to start the
     * background job
     */
    @Activate
    public void activate(ComponentContext context) {
        logger.trace("activate: start");

        // set the ClusterStateChangeListener with the DocumentNodeStore
        this.documentNodeStore = (DocumentNodeStore) nodeStore;
        documentNodeStore.setClusterStateChangeListener(this);

        // retrieve the clusterId
        clusterNodeId = documentNodeStore.getClusterId();

        // start the background worker
        backgroundWorker = new BackgroundWorker();
        Thread th = new Thread(backgroundWorker, "DocumentDiscoveryLiteService-BackgroundWorker-[" + clusterNodeId + "]");
        th.setDaemon(true);
        th.start();

        // register the Descriptors - for Oak to pass it upwards
        if (context != null) {
            OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
            whiteboard.register(Descriptors.class, new DiscoveryLiteDescriptor(), Collections.emptyMap());
        }
        logger.trace("activate: end");
    }

    /**
     * On deactivate the background job is stopped - if it was running at all
     **/
    @Deactivate
    protected void deactivate() {
        logger.trace("deactivate: deactivated");

        if (backgroundWorker != null) {
            backgroundWorker.stop();
            backgroundWorker = null;
        }
        logger.trace("deactivate: end");
    }

    /**
     * Checks if anything changed in the current view and updates the service
     * fields accordingly.
     * 
     * @return true if anything changed or is about to be changed (eg
     *         recovery/backlog), false if the view is stable
     */
    private boolean checkView() {
        logger.trace("checkView: start");
        List<ClusterNodeInfoDocument> allClusterNodes = ClusterNodeInfoDocument.all(documentNodeStore.getDocumentStore());

        Map<Integer, ClusterNodeInfoDocument> allNodeIds = new HashMap<Integer, ClusterNodeInfoDocument>();
        Map<Integer, ClusterNodeInfoDocument> activeNotTimedOutNodes = new HashMap<Integer, ClusterNodeInfoDocument>();
        Map<Integer, ClusterNodeInfoDocument> activeButTimedOutNodes = new HashMap<Integer, ClusterNodeInfoDocument>();
        Map<Integer, ClusterNodeInfoDocument> recoveringNodes = new HashMap<Integer, ClusterNodeInfoDocument>();
        Map<Integer, ClusterNodeInfoDocument> backlogNodes = new HashMap<Integer, ClusterNodeInfoDocument>();
        Map<Integer, ClusterNodeInfoDocument> inactiveNoBacklogNodes = new HashMap<Integer, ClusterNodeInfoDocument>();

        for (Iterator<ClusterNodeInfoDocument> it = allClusterNodes.iterator(); it.hasNext();) {
            ClusterNodeInfoDocument clusterNode = it.next();
            allNodeIds.put(clusterNode.getClusterId(), clusterNode);
            if (clusterNode.isBeingRecovered()) {
                recoveringNodes.put(clusterNode.getClusterId(), clusterNode);
            } else if (!clusterNode.isActive()) {
                if (hasBacklog(clusterNode)) {
                    backlogNodes.put(clusterNode.getClusterId(), clusterNode);
                } else {
                    inactiveNoBacklogNodes.put(clusterNode.getClusterId(), clusterNode);
                }
            } else if (clusterNode.getLeaseEndTime() < System.currentTimeMillis()) {
                activeButTimedOutNodes.put(clusterNode.getClusterId(), clusterNode);
            } else {
                activeNotTimedOutNodes.put(clusterNode.getClusterId(), clusterNode);
            }
        }

        // the current view should now consist of:
        // activeNotTimedOutNodes and activeButTimedOutNodes!
        // (reason for including the timedout: they will yet have to
        // switch to recovering or inactive - but we DONT KNOW yet.. that's
        // predicting the future - so so far we have to stick with
        // including them in the view)
        Map<Integer, ClusterNodeInfoDocument> allActives;
        allActives = new HashMap<Integer, ClusterNodeInfoDocument>(activeNotTimedOutNodes);
        allActives.putAll(activeButTimedOutNodes);

        // terminology:
        // 'inactivating' are nodes that are either 'recovering' or 'backlog'
        // ones
        // 'recovering' are nodes for which one node is doing the recover() of
        // lastRevs
        // 'backlog' ones are nodes that are no longer active, that have
        // finished the
        // recover() but for which a backgroundRead is still pending to read
        // the latest root changes.

        logger.debug(
                "checkView: active nodes: {}, timed out nodes: {}, recovering nodes: {}, backlog nodes: {}, inactive nodes: {}, total: {}, hence view nodes: {}",
                activeNotTimedOutNodes.size(), activeButTimedOutNodes.size(), recoveringNodes.size(), backlogNodes.size(),
                inactiveNoBacklogNodes.size(), allNodeIds.size(), allActives.size());

        ClusterViewDocument originalView = previousClusterViewDocument;
        ClusterViewDocument newView = doCheckView(allActives.keySet(), recoveringNodes.keySet(), backlogNodes.keySet(),
                inactiveNoBacklogNodes.keySet());
        if (newView == null) {
            logger.trace("checkView: end. newView: null");
            return true;
        }
        boolean newHasInstancesWithBacklog = recoveringNodes.size() > 0 || backlogNodes.size() > 0;
        boolean changed = originalView == null || (newView.getViewSeqNum() != originalView.getViewSeqNum())
                || (newHasInstancesWithBacklog != hasInstancesWithBacklog);
        logger.debug("checkView: viewFine: {}, changed: {}, originalView: {}, newView: {}", newView != null, changed, originalView,
                newView);

        if (longTimeInactives.addAll(inactiveNoBacklogNodes.keySet())) {
            logger.debug("checkView: updated longTimeInactives to {} (inactiveNoBacklogNodes: {})", longTimeInactives,
                    inactiveNoBacklogNodes);
        }

        if (changed) {
            String clusterId = ClusterRepositoryInfo.getOrCreateId(documentNodeStore);
            ClusterView v = ClusterView.fromDocument(clusterNodeId, clusterId, newView, backlogNodes.keySet());
            ClusterView previousView = previousClusterView;
            previousClusterView = v;
            hasInstancesWithBacklog = newHasInstancesWithBacklog;
            logger.info("checkView: view changed from: " + previousView + ", to: " + v + ", hasInstancesWithBacklog: "
                    + hasInstancesWithBacklog);
            return true;
        } else {
            logger.debug("checkView: no changes whatsoever, still at view: " + previousClusterView);
            return hasInstancesWithBacklog;
        }
    }

    private Revision getLastKnownRevision(int clusterNodeId) {
        String[] lastKnownRevisions = documentNodeStore.getMBean().getLastKnownRevisions();
        for (int i = 0; i < lastKnownRevisions.length; i++) {
            String aLastKnownRevisionStr = lastKnownRevisions[i];
            String[] split = aLastKnownRevisionStr.split("=");
            if (split.length == 2) {
                try {
                    Integer id = Integer.parseInt(split[0]);
                    if (id == clusterNodeId) {
                        final Revision lastKnownRev = Revision.fromString(split[1]);
                        logger.trace("getLastKnownRevision: end. clusterNode: {}, lastKnownRevision: {}", clusterNodeId,
                                lastKnownRev);
                        return lastKnownRev;
                    }
                } catch (NumberFormatException nfe) {
                    logger.warn("getLastKnownRevision: could not parse integer '" + split[0] + "': " + nfe, nfe);
                }
            } else {
                logger.warn("getLastKnownRevision: cannot parse lastKnownRevision: " + aLastKnownRevisionStr);
            }
        }
        logger.warn("getLastKnownRevision: no lastKnownRevision found for " + clusterNodeId);
        return null;
    }

    private boolean hasBacklog(ClusterNodeInfoDocument clusterNode) {
        if (logger.isTraceEnabled()) {
            logger.trace("hasBacklog: start. clusterNodeId: {}", clusterNode.getClusterId());
        }
        Revision lastKnownRevision = getLastKnownRevision(clusterNode.getClusterId());
        if (lastKnownRevision == null) {
            logger.warn("hasBacklog: no lastKnownRevision found, hence cannot determine backlog for node "
                    + clusterNode.getClusterId());
            return false;
        }

        // The lastKnownRevision is what the local instance has last read/seen
        // from another instance.
        // This must be compared to what the other instance *actually* has
        // written as the very last thing.
        // Now the knowledge what the other instance has last written (after
        // recovery) would sit
        // in the root document - so that could in theory be used. But reading
        // the root document
        // would have to be done *uncached*. And that's quite a change to what
        // the original
        // idea was: that the root document would only be read every second, to
        // avoid contention.
        // So this 'what the other instance has last written' information is
        // retrieved via
        // a new, dedicated property in the clusterNodes collection: the
        // 'lastWrittenRootRev'.
        // The 'lastWrittenRootRev' is written by 'UnsavedModifications' during
        // backgroundUpdate
        // and retrieved here quite regularly (but it should not be a big deal,
        // as the
        // discovery-lite is the only one reading this field so frequently and
        // it does not
        // interfere with normal (jcr) nodes at all).
        String lastWrittenRootRevStr = clusterNode.getLastWrittenRootRev();
        if (lastWrittenRootRevStr == null) {
            boolean warn = false;
            Object oakVersion = clusterNode.get(ClusterNodeInfo.OAK_VERSION_KEY);
            if (oakVersion!=null && (oakVersion instanceof String)) {
                try{
                    Version actual = Version.parseVersion((String) oakVersion);
                    Version introduced = Version.parseVersion("1.3.5");
                    if (actual.compareTo(introduced)>=0) {
                        warn = true;
                    }
                } catch(Exception e) {
                    logger.debug("hasBacklog: couldn't parse version "+oakVersion+" : "+e);
                    warn = true;
                }
            }
            if (warn) {
                logger.warn("hasBacklog: node has lastWrittenRootRev=null");
            } else {
                logger.debug("hasBacklog: node has lastWrittenRootRev=null");
            }
            return false;
        }
        Revision lastWrittenRootRev = Revision.fromString(lastWrittenRootRevStr);
        if (lastWrittenRootRev == null) {
            logger.warn("hasBacklog: node has no lastWrittenRootRev: " + clusterNode.getClusterId());
            return false;
        }

        boolean hasBacklog = Revision.getTimestampDifference(lastKnownRevision, lastWrittenRootRev) < 0;
        if (logger.isDebugEnabled()) {
            logger.debug("hasBacklog: clusterNodeId: {}, lastKnownRevision: {}, lastWrittenRootRev: {}, hasBacklog: {}",
                    clusterNode.getClusterId(), lastKnownRevision, lastWrittenRootRev, hasBacklog);
        }
        return hasBacklog;
    }

    private ClusterViewDocument doCheckView(final Set<Integer> activeNodes, final Set<Integer> recoveringNodes,
            final Set<Integer> backlogNodes, final Set<Integer> inactiveNodes) {
        logger.trace("doCheckView: start: activeNodes: {}, recoveringNodes: {}, backlogNodes: {}, inactiveNodes: {}", activeNodes,
                recoveringNodes, backlogNodes, inactiveNodes);

        Set<Integer> allInactives = new HashSet<Integer>();
        allInactives.addAll(inactiveNodes);
        allInactives.addAll(backlogNodes);

        if (activeNodes.size() == 0) {
            // then we have zero active nodes - that's nothing expected as that
            // includes our own node not to be active
            // hence handle with care - ie wait until we get an active node
            logger.warn("doCheckView: empty active ids. activeNodes:{}, recoveringNodes:{}, inactiveNodes:{}", activeNodes,
                    recoveringNodes, inactiveNodes);
            return null;
        }
        ClusterViewDocument newViewOrNull;
        try {
            newViewOrNull = ClusterViewDocument.readOrUpdate(documentNodeStore, activeNodes, recoveringNodes, allInactives);
        } catch (RuntimeException re) {
            logger.error("doCheckView: RuntimeException: re: " + re, re);
            return null;
        } catch (Error er) {
            logger.error("doCheckView: Error: er: " + er, er);
            return null;
        }
        logger.trace("doChckView: readOrUpdate result: {}", newViewOrNull);

        // and now for some verbose documentation and logging:
        if (newViewOrNull == null) {
            // then there was a concurrent update of the clusterView
            // and we should do some quick backoff sleeping
            logger.debug("doCheckView: newViewOrNull is null: " + newViewOrNull);
            return null;
        } else {
            // otherwise we now hold the newly valid view
            // it could be the same or different to the previous one, let's
            // check
            if (previousClusterViewDocument == null) {
                // oh ok, this is the very first one
                previousClusterViewDocument = newViewOrNull;
                logger.debug("doCheckView: end. first ever view: {}", newViewOrNull);
                return newViewOrNull;
            } else if (previousClusterViewDocument.getViewSeqNum() == newViewOrNull.getViewSeqNum()) {
                // that's the normal case: the viewId matches, nothing has
                // changed, we've already
                // processed the previousClusterView, so:
                logger.debug("doCheckView: end. seqNum did not change. view: {}", newViewOrNull);
                return newViewOrNull;
            } else {
                // otherwise the view has changed
                logger.info("doCheckView: view has changed from: {} to: {} - sending event...", previousClusterViewDocument,
                        newViewOrNull);
                previousClusterViewDocument = newViewOrNull;
                logger.debug("doCheckView: end. changed view: {}", newViewOrNull);
                return newViewOrNull;
            }
        }
    }

    @Override
    public void handleClusterStateChange() {
        // handleClusterStateChange is needed to learn about any state change in
        // the clusternodes
        // collection asap and being able to react on it - so this will wake up
        // the
        // backgroundWorker which in turn will - in a separate thread - check
        // the view
        // and send out events accordingly
        wakeupBackgroundWorker(WakeupReason.CLUSTER_STATE_CHANGED);
    }

    private void wakeupBackgroundWorker(WakeupReason wakeupReason) {
        final BackgroundWorker bw = backgroundWorker;
        if (bw != null) {
            // get a copy of this.hasInstancesWithBacklog for just the code-part
            // in this synchronized
            boolean hasInstancesWithBacklog = this.hasInstancesWithBacklog;

            if (wakeupReason == WakeupReason.BACKGROUND_READ_FINISHED) {
                // then only forward the notify if' hasInstancesWithBacklog'
                // ie, we have anything we could be waiting for - otherwise
                // we dont need to wakeup the background thread
                if (!hasInstancesWithBacklog) {
                    logger.trace(
                            "wakeupBackgroundWorker: not waking up backgroundWorker, as we do not have any instances with backlog");
                    return;
                }
            }
            logger.trace("wakeupBackgroundWorker: waking up backgroundWorker, reason: {} (hasInstancesWithBacklog: {})",
                    wakeupReason, hasInstancesWithBacklog);
            synchronized (bw) {
                bw.notifyAll();
            }
        }
    }

    /**
     * <p>
     * Additionally the DocumentDiscoveryLiteService must be notified when the
     * background-read has finished - as it could be waiting for a crashed
     * node's recovery to finish - which it can only do by checking the
     * lastKnownRevision of the crashed instance - and that check is best done
     * after the background read is just finished (it could optinoally do that
     * just purely time based as well, but going via a listener is more timely,
     * that's why this approach has been chosen).
     */
    @Override
    public void contentChanged(@Nonnull NodeState root,@Nonnull CommitInfo info) {
        // contentChanged is only used to react as quickly as possible
        // when we have instances that have a 'backlog' - ie when instances
        // crashed
        // and are being recovered - then we must wait until the recovery is
        // finished
        // AND until the subsequent background read actually reads that
        // instance'
        // last changes. To catch that moment as quickly as possible,
        // this contentChanged is used.
        // Now from the above it also results that this only wakes up the
        // backgroundWorker if we have any pending 'backlogy instances'
        // otherwise this is a no-op
        if (info.isExternal()) {
            // then ignore this as this is likely an external change
            // note: it could be a compacted change, in which case we should
            // probably still process it - but we have a 5sec fallback
            // in the BackgroundWorker to handle that case too,
            // so:
            logger.trace("contentChanged: ignoring content change due to commit info belonging to external change");
            return;
        }
        logger.trace("contentChanged: handling content changed by waking up worker if necessary");
        wakeupBackgroundWorker(WakeupReason.BACKGROUND_READ_FINISHED);
    }

}
