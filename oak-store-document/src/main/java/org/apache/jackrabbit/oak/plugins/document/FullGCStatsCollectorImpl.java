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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.GCPhase;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.GaugeStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

/**
 * {@link DocumentNodeStore} Full garbage collection statistics.
 */
class FullGCStatsCollectorImpl implements FullGCStatsCollector {

    static final String OAK_RUN_METRICS_PREFIX = "oak_FullGC";
    static final String FULL_GC = "FullGC";
    static final String READ_DOC = "READ_DOC";
    static final String DELETED_ORPHAN_NODE = "DELETED_ORPHAN_NODE";
    static final String DELETED_PROPERTY = "DELETED_PROPERTY";
    static final String DELETED_INTERNAL_PROPERTY = "DELETED_INTERNAL_PROPERTY";
    static final String DELETED_UNMERGED_BC = "DELETED_UNMERGED_BC";
    static final String DELETED_REVISION = "DELETED_REVISION";
    static final String UPDATED_DOC = "UPDATED_DOC";
    static final String SKIPPED_DOC = "SKIPPED_DOC";
    static final String FULL_GC_ACTIVE_TIMER = "FULL_GC_ACTIVE_TIMER";
    static final String FULL_GC_TIMER = "FULL_GC_TIMER";
    static final String COLLECT_FULL_GC_TIMER = "COLLECT_FULL_GC_TIMER";
    static final String COLLECT_ORPHAN_NODES_TIMER = "COLLECT_ORPHAN_NODES_TIMER";
    static final String COLLECT_DELETED_PROPS_TIMER = "COLLECT_DELETED_PROPS_TIMER";
    static final String COLLECT_DELETED_OLD_REVS_TIMER = "COLLECT_DELETED_OLD_REVS_TIMER";
    static final String COLLECT_UNMERGED_BC_TIMER = "COLLECT_UNMERGED_BC_TIMER";
    static final String DELETE_FULL_GC_DOCS_TIMER = "DELETE_FULL_GC_DOCS_TIMER";

    static final String COUNTER = "COUNTER";
    static final String FAILURE_COUNTER = "FAILURE";

    static final String ENABLED = "ENABLED";
    static final String MODE = "MODE";
    static final String DELAY_FACTOR = "DELAY_FACTOR";
    static final String BATCH_SIZE = "BATCH_SIZE";
    static final String PROGRESS_SIZE = "PROGRESS_SIZE";
    static final String EMBEDDED_VERIFICATION_ENABLED = "EMBEDDED_VERIFICATION_ENABLED";
    static final String MAX_AGE = "MAX_AGE";
    static final String FULL_GC_GENERATION = "FULL_GC_GENERATION";

    private final StatisticsProvider provider;

    private final MeterStats readDoc;
    private final MeterStats deletedOrphanNode;
    private final MeterStats deletedProperty;
    private final MeterStats deletedUnmergedBC;
    private final MeterStats updatedDoc;
    private final MeterStats skippedDoc;

    private final Map<GCPhase, MeterStats> candidateRevisions;
    private final Map<GCPhase, MeterStats> candidateInternalRevisions;
    private final Map<GCPhase, MeterStats> candidateProperties;
    private final Map<GCPhase, MeterStats> candidateDocuments;

    private final TimerStats fullGCActiveTimer;
    private final TimerStats fullGCTimer;
    private final TimerStats collectFullGCTimer;
    private final TimerStats collectOrphanNodesTimer;
    private final TimerStats collectDeletedPropsTimer;
    private final TimerStats collectDeletedOldRevsTimer;
    private final TimerStats collectUnmergedBCTimer;
    private final TimerStats deleteFullGCDocsTimer;

    private final CounterStats counter;
    private final CounterStats failureCounter;
    private static String METRICS_QUALIFIED_NAME_PREFIX;

    // FullGC OSGi config stats
    private GaugeStats<Boolean> enabled;
    private GaugeStats<Integer> mode;
    private GaugeStats<Double> delayFactor;
    private GaugeStats<Integer> batchSize;
    private GaugeStats<Integer> progressSize;
    private GaugeStats<Boolean> embeddedVerificationEnabled;
    private GaugeStats<Long> maxAge;
    private GaugeStats<Long> fullGCGeneration;

    FullGCStatsCollectorImpl(StatisticsProvider provider) {
        this(provider, false);
    }

    FullGCStatsCollectorImpl(StatisticsProvider provider, boolean isOakRunJob) {
        this.provider = provider;
        this.METRICS_QUALIFIED_NAME_PREFIX = isOakRunJob ? OAK_RUN_METRICS_PREFIX : FULL_GC;

        readDoc = meter(provider, READ_DOC);
        deletedOrphanNode = meter(provider, DELETED_ORPHAN_NODE);
        deletedProperty = meter(provider, DELETED_PROPERTY);
        deletedUnmergedBC = meter(provider, DELETED_UNMERGED_BC);
        updatedDoc = meter(provider, UPDATED_DOC);
        skippedDoc = meter(provider, SKIPPED_DOC);

        candidateRevisions = new EnumMap<>(GCPhase.class);
        candidateInternalRevisions = new EnumMap<>(GCPhase.class);
        candidateProperties = new EnumMap<>(GCPhase.class);
        candidateDocuments = new EnumMap<>(GCPhase.class);

        fullGCActiveTimer = timer(provider, FULL_GC_ACTIVE_TIMER);
        fullGCTimer = timer(provider, FULL_GC_TIMER);
        collectFullGCTimer = timer(provider, COLLECT_FULL_GC_TIMER);
        collectOrphanNodesTimer = timer(provider, COLLECT_ORPHAN_NODES_TIMER);
        collectDeletedPropsTimer = timer(provider, COLLECT_DELETED_PROPS_TIMER);
        collectDeletedOldRevsTimer = timer(provider, COLLECT_DELETED_OLD_REVS_TIMER);
        collectUnmergedBCTimer = timer(provider, COLLECT_UNMERGED_BC_TIMER);
        deleteFullGCDocsTimer = timer(provider, DELETE_FULL_GC_DOCS_TIMER);

        counter = counter(provider, COUNTER);
        failureCounter = counter(provider, FAILURE_COUNTER);
    }

    //---------------------< FullGCStatsCollector >-------------------------

    @Override
    public void documentRead() {
        readDoc.mark();
    }

    @Override
    public void candidateProperties(GCPhase phase, long numProps) {
        getMeter(candidateProperties, phase, DELETED_PROPERTY).mark(numProps);
    }

    @Override
    public void candidateDocuments(GCPhase phase, long numDocs) {
        getMeter(candidateDocuments, phase, DELETED_UNMERGED_BC).mark(numDocs);
    }

    @Override
    public void candidateRevisions(GCPhase phase, long numRevs) {
        getMeter(candidateRevisions, phase, DELETED_REVISION).mark(numRevs);
    }

    @Override
    public void candidateInternalRevisions(GCPhase phase, long numRevs) {
        getMeter(candidateInternalRevisions, phase, DELETED_INTERNAL_PROPERTY).mark(numRevs);
    }

    @Override
    public void orphanNodesDeleted(long numNodes) {
        deletedOrphanNode.mark(numNodes);
    }

    @Override
    public void propertiesDeleted(long numProps) {
        deletedProperty.mark(numProps);
    }

    @Override
    public void unmergedBranchCommitsDeleted(long numCommits) {
        deletedUnmergedBC.mark(numCommits);
    }

    @Override
    public void documentsUpdated(long numDocs) {
        updatedDoc.mark(numDocs);
    }

    @Override
    public void documentsUpdateSkipped(long numDocs) {
        skippedDoc.mark(numDocs);
    }

    @Override
    public void started() {
        counter.inc();
    }

    @Override
    public void finished(VersionGCStats stats) {
        fullGCActiveTimer.update(stats.fullGCActiveElapsed, MICROSECONDS);
        fullGCTimer.update(stats.fullGCDocsElapsed, MICROSECONDS);
        collectFullGCTimer.update(stats.collectFullGCElapsed, MICROSECONDS);
        collectOrphanNodesTimer.update(stats.collectOrphanNodesElapsed, MICROSECONDS);
        collectDeletedPropsTimer.update(stats.collectDeletedPropsElapsed, MICROSECONDS);
        collectDeletedOldRevsTimer.update(stats.collectDeletedOldRevsElapsed, MICROSECONDS);
        collectUnmergedBCTimer.update(stats.collectUnmergedBCElapsed, MICROSECONDS);
        deleteFullGCDocsTimer.update(stats.deleteFullGCDocsElapsed, MICROSECONDS);
        if (!stats.success) {
            failureCounter.inc();
        }
    }

    @Override
    public void enabled(boolean enabled) {
        this.enabled = gauge(provider, ENABLED, () -> enabled);
    }

    @Override
    public void mode(int mode) {
        this.mode = gauge(provider, MODE, () -> mode);
    }

    @Override
    public void verificationEnabled(boolean verificationEnabled) {
        this.embeddedVerificationEnabled = gauge(provider, EMBEDDED_VERIFICATION_ENABLED, () -> verificationEnabled);
    }

    @Override
    public void delayFactor(double delayFactor) {
        this.delayFactor = gauge(provider, DELAY_FACTOR, () -> delayFactor);
    }

    @Override
    public void batchSize(int batchSize) {
        this.batchSize = gauge(provider, BATCH_SIZE, () -> batchSize);
    }

    @Override
    public void progressSize(int progressSize) {
        this.progressSize = gauge(provider, PROGRESS_SIZE, () -> progressSize);
    }

    @Override
    public void maxAge(long maxAge) {
        this.maxAge = gauge(provider, MAX_AGE, () -> maxAge);
    }

    @Override
    public void fullGCGeneration(long generation) {
        this.fullGCGeneration = gauge(provider, FULL_GC_GENERATION, () -> generation);
    }

    @Override
    public String toString() {
        return "FullGCStatsCollectorImpl{" +
                "enabled=" + getValue(enabled, "false") +
                ", mode=" + getValue(mode, "0") +
                ", delayFactor=" + getValue(delayFactor, "0.0") +
                ", batchSize=" + getValue(batchSize, "0") +
                ", progressSize=" + getValue(progressSize, "0") +
                ", embeddedVerificationEnabled=" + getValue(embeddedVerificationEnabled, "false") +
                ", maxAge=" + getValue(maxAge, "0") +
                ", fullGCGeneration=" + getValue(fullGCGeneration, "0") +
                ", readDoc=" + readDoc.getCount() +
                ", candidateRevisions=" + mapToString(candidateRevisions) +
                ", candidateInternalRevisions=" + mapToString(candidateInternalRevisions) +
                ", candidateProperties=" + mapToString(candidateProperties) +
                ", candidateDocuments=" + mapToString(candidateDocuments) +
                ", deletedOrphanNode=" + deletedOrphanNode.getCount() +
                ", deletedProperty=" + deletedProperty.getCount() +
                ", deletedUnmergedBC=" + deletedUnmergedBC.getCount() +
                ", updatedDoc=" + updatedDoc.getCount() +
                ", skippedDoc=" + skippedDoc.getCount() +
                '}';
    }

    //----------------------------< internal >----------------------------------

    private String mapToString(Map<GCPhase, MeterStats> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean isFirst = true;
        for (Map.Entry<GCPhase, MeterStats> entry : map.entrySet()) {
            if (!isFirst) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue().getCount());
            isFirst = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static MeterStats meter(StatisticsProvider provider, String name) {
        return provider.getMeter(qualifiedName(name), DEFAULT);
    }

    private static TimerStats timer(StatisticsProvider provider, String name) {
        return provider.getTimer(qualifiedName(name), METRICS_ONLY);
    }

    private static CounterStats counter(StatisticsProvider provider, String name) {
        return provider.getCounterStats(qualifiedName(name), METRICS_ONLY);
    }

    private static <T> GaugeStats<T> gauge(StatisticsProvider provider, String name, Supplier<T> value) {
        return provider.getGauge(qualifiedName(name), value);
    }

    private static String qualifiedName(String metricName) {
        return METRICS_QUALIFIED_NAME_PREFIX + "." + metricName;
    }

    private MeterStats getMeter(Map<GCPhase, MeterStats> map, GCPhase phase, String name) {
        return map.computeIfAbsent(phase, p -> meter(provider, name + "." + p.name()));
    }

    private String getValue(final GaugeStats<?> gaugeStats, final String defaultValue) {
        return gaugeStats != null ? String.valueOf(gaugeStats.getValue()) : defaultValue;
    }

}
