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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.toArray;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;

/**
 * Implementation of a DocumentNodeStoreMBean.
 */
final class DocumentNodeStoreMBeanImpl extends AnnotatedStandardMBean implements DocumentNodeStoreMBean {

    private static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");
    private static final String COMPOSITE_INFO = "composite.checkpoint.";

    private final DocumentNodeStore nodeStore;
    private final RepositoryStatistics repoStats;
    private final Iterable<ClusterNodeInfoDocument> clusterNodes;
    private final long revisionGCMaxAgeMillis;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    DocumentNodeStoreMBeanImpl(DocumentNodeStore nodeStore,
                               RepositoryStatistics repoStats,
                               Iterable<ClusterNodeInfoDocument> clusterNodes,
                               long revisionGCMaxAgeMillis) {
        super(DocumentNodeStoreMBean.class);
        this.nodeStore = nodeStore;
        this.repoStats = repoStats;
        this.clusterNodes = clusterNodes;
        this.revisionGCMaxAgeMillis = revisionGCMaxAgeMillis;
    }

    @Override
    public String getRevisionComparatorState() {
        return "";
    }

    @Override
    public String getHead() {
        return nodeStore.getHeadRevision().toString();
    }

    @Override
    public int getClusterId() {
        return nodeStore.getClusterId();
    }

    @Override
    public int getUnmergedBranchCount() {
        return nodeStore.getBranches().size();
    }

    @Override
    public String[] getInactiveClusterNodes() {
        return toArray(transform(filter(clusterNodes,
                input -> !input.isActive()),
                input -> input.getClusterId() + "=" + input.getCreated()), String.class);
    }

    @Override
    public String[] getActiveClusterNodes() {
        return toArray(transform(filter(clusterNodes,
                input -> input.isActive()),
                input -> input.getClusterId() + "=" + input.getLeaseEndTime()), String.class);
    }

    @Override
    public String[] getLastKnownRevisions() {
        return toArray(transform(filter(nodeStore.getHeadRevision(),
                input -> input.getClusterId() != getClusterId()),
                input -> input.getClusterId() + "=" + input.toString()), String.class);
    }

    @Override
    public String formatRevision(String rev, boolean utc) {
        Revision r = Revision.fromString(rev);
        final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
        if (utc) {
            sdf.setTimeZone(TZ_UTC);
        }
        return sdf.format(r.getTimestamp());
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return nodeStore.getDocumentStore().determineServerTimeDifferenceMillis();
    }

    @Override
    public CompositeData getMergeSuccessHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.MERGE_SUCCESS_COUNT,
                "Merge Success Count");
    }

    @Override
    public CompositeData getMergeFailureHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.MERGE_FAILED_EXCLUSIVE,
                "Merge failure count");
    }

    @Override
    public CompositeData getExternalChangeCountHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.BGR_NUM_CHANGES_RATE,
                "Count of nodes modified by other " +
                        "cluster nodes since last background read");
    }

    @Override
    public CompositeData getBackgroundUpdateCountHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.BGW_NUM_WRITES_RATE,
                "Count of nodes updated as part of " +
                        "background update");
    }

    @Override
    public CompositeData getBranchCommitHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.BRANCH_COMMIT_COUNT,
                "Branch commit count");
    }

    @Override
    public CompositeData getMergeBranchCommitHistory() {
        return getTimeSeriesData(DocumentNodeStoreStats.MERGE_BRANCH_COMMIT_COUNT,
                "Number of merged branch commits");
    }

    private CompositeData getTimeSeriesData(String name, String desc) {
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    private TimeSeries getTimeSeries(String name) {
        return repoStats.getTimeSeries(name, true);
    }

    @Override
    public int recover(String path, int clusterId) {
        requireNonNull(path, "path must not be null");
        checkArgument(PathUtils.isAbsolute(path), "path must be absolute");
        checkArgument(clusterId >= 0, "clusterId must not be a negative");

        DocumentStore docStore = nodeStore.getDocumentStore();
        boolean isActive = false;

        for (ClusterNodeInfoDocument it : ClusterNodeInfoDocument.all(docStore)) {
            if (it.getClusterId() == clusterId && it.isActive()) {
                isActive = true;
            }
        }

        if (isActive) {
            throw new IllegalStateException(
                    "Cannot run recover on clusterId " + clusterId + " as it's currently active");
        }

        String p = path;
        NodeDocument nodeDocument = docStore.find(Collection.NODES, Utils.getIdFromPath(p));
        if(nodeDocument == null) {
            throw new DocumentStoreException("Document node with given path = " + p + " does not exist");
        }

        boolean dryRun = nodeStore.isReadOnlyMode();
        int sum = 0;
        for (;;) {
            log.info("Running recovery on child documents of path = " + p);
            List<NodeDocument> childDocs = getChildDocs(p);
            sum += nodeStore.getLastRevRecoveryAgent().recover(childDocs, clusterId, dryRun);
            if (PathUtils.denotesRoot(p)) {
                break;
            }
            p = PathUtils.getParentPath(p);
        }
        return sum;
    }

    private List<NodeDocument> getChildDocs(String path) { 
        Path pathRef = Path.fromString(path);
        final String to = Utils.getKeyUpperLimit(pathRef);
        final String from = Utils.getKeyLowerLimit(pathRef);
        return nodeStore.getDocumentStore().query(Collection.NODES, from, to, 10000);
    }

    @Override
    public String cleanAllCaches() {
        nodeStore.getDiffCache().invalidateAll();
        nodeStore.getNodeCache().invalidateAll();
        nodeStore.getNodeChildrenCache().invalidateAll();
        nodeStore.getDocumentStore().invalidateCache();
        return "Caches invalidated.";
    }

    @Override
    public String cleanIndividualCache(String name) {
        switch(name.toUpperCase()) {
            case "DIFF":
                nodeStore.getDiffCache().invalidateAll();
                return "DiffCache invalidated.";
            case "NODE":
                nodeStore.getNodeCache().invalidateAll();
                return "NodeCache invalidated.";
            case "NODECHILDREN":
                nodeStore.getNodeChildrenCache().invalidateAll();
                return "NodeChildrenCache invalidated.";
            case "DOCUMENT":
                nodeStore.getDocumentStore().invalidateCache();
                return "DocumentCache invalidated.";
            default:
                return "ERROR: Invalid cache name received.";
        }
    }

    @Override
    public String createCheckpoint(String revision, long lifetime, boolean force) {
        Revision rev = Revision.fromString(revision);
        long oldestTimestamp = nodeStore.getClock().getTime() - revisionGCMaxAgeMillis;
        Revision oldestCheckpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
        if (oldestCheckpoint != null) {
            oldestTimestamp = Math.min(oldestTimestamp, oldestCheckpoint.getTimestamp());
        }
        if (force || oldestTimestamp < rev.getTimestamp()) {
            Map<String, String> info = new HashMap<>();
            // Below properties are only needed when the DocumentNodeStore is
            // used in a composite NodeStore setup. This is a bit ugly because
            // it introduces a dependency on an implementation detail of the
            // CompositeNodeStore implementation. The module oak-it therefore
            // has a CompositeCheckpointTest to prevent a regression should the
            // implementation ever change.
            info.put(COMPOSITE_INFO + "created", Long.toString(rev.getTimestamp()));
            info.put(COMPOSITE_INFO + "expires", Long.toString(Utils.sum(rev.getTimestamp() + lifetime)));

            String cp = nodeStore.getCheckpoints().create(lifetime, info, rev).toString();
            log.info("Created checkpoint [{}] with lifetime {} for Revision {}", cp, lifetime, revision);
            return String.format("Created checkpoint [%s] with lifetime %d for Revision %s", cp, lifetime, revision);
        } else {
            throw new IllegalArgumentException(String.format("Cannot create a checkpoint for revision %s. " +
                    "Revision timestamp is %d and oldest timestamp to keep is %d",
                    revision, rev.getTimestamp(), oldestTimestamp));
        }
    }

}
