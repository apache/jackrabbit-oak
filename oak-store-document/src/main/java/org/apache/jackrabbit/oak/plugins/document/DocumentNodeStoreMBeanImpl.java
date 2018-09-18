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
import java.util.TimeZone;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;

/**
 * Implementation of a DocumentNodeStoreMBean.
 */
final class DocumentNodeStoreMBeanImpl extends AnnotatedStandardMBean implements DocumentNodeStoreMBean {

    private static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");

    private final DocumentNodeStore nodeStore;
    private final RepositoryStatistics repoStats;
    private final Iterable<ClusterNodeInfoDocument> clusterNodes;

    DocumentNodeStoreMBeanImpl(DocumentNodeStore nodeStore,
                               RepositoryStatistics repoStats,
                               Iterable<ClusterNodeInfoDocument> clusterNodes)
            throws NotCompliantMBeanException {
        super(DocumentNodeStoreMBean.class);
        this.nodeStore = nodeStore;
        this.repoStats = repoStats;
        this.clusterNodes = clusterNodes;
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
                new Predicate<ClusterNodeInfoDocument>() {
            @Override
            public boolean apply(ClusterNodeInfoDocument input) {
                return !input.isActive();
            }
        }), new Function<ClusterNodeInfoDocument, String>() {
            @Override
            public String apply(ClusterNodeInfoDocument input) {
                return input.getClusterId() + "=" + input.getCreated();
            }
        }), String.class);
    }

    @Override
    public String[] getActiveClusterNodes() {
        return toArray(transform(filter(clusterNodes,
                new Predicate<ClusterNodeInfoDocument>() {
            @Override
            public boolean apply(ClusterNodeInfoDocument input) {
                return input.isActive();
            }
        }), new Function<ClusterNodeInfoDocument, String>() {
            @Override
            public String apply(ClusterNodeInfoDocument input) {
                return input.getClusterId() + "=" + input.getLeaseEndTime();
            }
        }), String.class);
    }

    @Override
    public String[] getLastKnownRevisions() {
        return toArray(transform(filter(nodeStore.getHeadRevision(),
                new Predicate<Revision>() {
            @Override
            public boolean apply(Revision input) {
                return input.getClusterId() != getClusterId();
            }
        }), new Function<Revision, String>() {
            @Override
            public String apply(Revision input) {
                return input.getClusterId() + "=" + input.toString();
            }
        }), String.class);
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
}
