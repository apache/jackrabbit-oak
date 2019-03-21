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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

public class DocumentNodeStoreStats implements DocumentNodeStoreStatsCollector {
    private static final String BGR_READ_HEAD = "DOCUMENT_NS_BGR_READ_HEAD";
    private static final String BGR_CACHE_INVALIDATE = "DOCUMENT_NS_BGR_CACHE_INVALIDATE";
    private static final String BGR_DIFF_CACHE = "DOCUMENT_NS_BGR_DIFF_CACHE";
    private static final String BGR_LOCK = "DOCUMENT_NS_BGR_LOCK";
    private static final String BGR_DISPATCH = "DOCUMENT_NS_BGR_DISPATCH";
    private static final String BGR_TOTAL_TIME = "DOCUMENT_NS_BGR_TOTAL_TIME";
    static final String BGR_NUM_CHANGES_RATE = "DOCUMENT_NS_BGR_NUM_CHANGES_RATE";
    private static final String BGR_NUM_CHANGES_HISTO = "DOCUMENT_NS_BGR_NUM_CHANGES_HISTO";
    static final String BGR_LAG = "DOCUMENT_NS_BGR_LAG";

    private static final String BGW_CLEAN = "DOCUMENT_NS_BGW_CLEAN";
    private static final String BGW_SPLIT = "DOCUMENT_NS_BGW_SPLIT";
    private static final String BGW_WRITE = "DOCUMENT_NS_BGW_LOCK";
    private static final String BGW_SWEEP = "DOCUMENT_NS_BGW_SWEEP";
    static final String BGW_NUM = "DOCUMENT_NS_BGW_NUM";
    static final String BGW_NUM_WRITES_RATE = "DOCUMENT_NS_BGW_NUM_WRITE_RATE";
    private static final String BGW_TOTAL = "DOCUMENT_NS_BGW_TOTAL_TIME";

    static final String LEASE_UPDATE = "DOCUMENT_NS_LEASE_UPDATE";

    private static final String MERGE_SUCCESS_NUM_RETRY = "DOCUMENT_NS_MERGE_SUCCESS_RETRY";
    static final String MERGE_SUCCESS_COUNT = "DOCUMENT_NS_MERGE_SUCCESS_COUNT";
    private static final String MERGE_SUCCESS_TIME = "DOCUMENT_NS_MERGE_SUCCESS_TIME";
    private static final String MERGE_SUCCESS_NORMALIZED_TIME = "DOCUMENT_NS_MERGE_SUCCESS_NORMALIZED_TIME";
    private static final String MERGE_SUCCESS_SUSPENDED = "DOCUMENT_NS_MERGE_SUCCESS_SUSPENDED";
    private static final String MERGE_SUCCESS_EXCLUSIVE = "DOCUMENT_NS_MERGE_SUCCESS_EXCLUSIVE";
    static final String MERGE_FAILED_EXCLUSIVE = "DOCUMENT_NS_MERGE_FAILED_EXCLUSIVE";
    private static final String HEAD_OF_QUEUE_WAIT_TIME = "DOCUMENT_NS_HEAD_OF_QUEUE_WAIT_TIME";
    private static final String MERGE_SUSPEND_TIME = "DOCUMENT_NS_MERGE_SUSPEND_TIME";
    private static final String MERGE_LOCK_TIME = "DOCUMENT_NS_MERGE_LOCK_TIME";
    private static final String MERGE_COMMIT_HOOK_TIME = "DOCUMENT_NS_MERGE_COMMIT_HOOK_TIME";
    private static final String MERGE_CHANGES_APPLIED_TIME = "DOCUMENT_NS_MERGE_CHANGES_APPLIED_TIME";
    private static final String MERGE_CHANGES_RATE = "DOCUMENT_NS_MERGE_CHANGES_RATE";

    static final String BRANCH_COMMIT_COUNT = "DOCUMENT_NS_BRANCH_COMMIT_COUNT";
    static final String MERGE_BRANCH_COMMIT_COUNT = "DOCUMENT_NS_MERGE_BRANCH_COMMIT_COUNT";

    // background read
    private final TimerStats readHead;
    private final TimerStats readCacheInvalidate;
    private final TimerStats readDiffCache;
    private final TimerStats readLock;
    private final TimerStats readDispatch;
    private final TimerStats readTotalTime;
    private final MeterStats numChangesRate;
    private final HistogramStats numChangesHisto;
    private final MeterStats changesLag;

    // background update
    private final TimerStats writeClean;
    private final TimerStats writeSplit;
    private final TimerStats writeSweep;
    private final HistogramStats writeNum;
    private final TimerStats writeWrite;
    private final TimerStats writeTotal;
    private final MeterStats numWritesRate;

    // lease update
    private final TimerStats leaseUpdate;

    // merge stats
    private final HistogramStats mergeSuccessRetries;
    private final MeterStats mergeSuccessRate;
    private final TimerStats mergeSuccessTime;
    private final TimerStats mergeSuccessNormalizedTime;
    private final MeterStats mergeSuccessExclusive;
    private final MeterStats mergeSuccessSuspended;
    private final MeterStats mergeFailedExclusive;
    private final TimerStats headOfQueueWaitTime;
    private final TimerStats mergeSuspendTime;
    private final TimerStats mergeLockTime;
    private final TimerStats mergeCommitHookTime;
    private final TimerStats mergeChangesApplied;
    private final MeterStats mergeChangesRate;

    // branch stats
    private final MeterStats branchCommitRate;
    private final MeterStats mergeBranchCommitRate;


    public DocumentNodeStoreStats(StatisticsProvider sp) {
        readHead = sp.getTimer(BGR_READ_HEAD, StatsOptions.METRICS_ONLY);
        readCacheInvalidate = sp.getTimer(BGR_CACHE_INVALIDATE, StatsOptions.METRICS_ONLY);
        readDiffCache = sp.getTimer(BGR_DIFF_CACHE, StatsOptions.METRICS_ONLY);
        readLock = sp.getTimer(BGR_LOCK, StatsOptions.METRICS_ONLY);
        readDispatch = sp.getTimer(BGR_DISPATCH, StatsOptions.METRICS_ONLY);
        readTotalTime = sp.getTimer(BGR_TOTAL_TIME, StatsOptions.METRICS_ONLY);
        numChangesRate = sp.getMeter(BGR_NUM_CHANGES_RATE, StatsOptions.DEFAULT); //Enable time series
        numChangesHisto = sp.getHistogram(BGR_NUM_CHANGES_HISTO, StatsOptions.METRICS_ONLY);
        changesLag = sp.getMeter(BGR_LAG, StatsOptions.METRICS_ONLY);

        writeClean = sp.getTimer(BGW_CLEAN, StatsOptions.METRICS_ONLY);
        writeSplit = sp.getTimer(BGW_SPLIT, StatsOptions.METRICS_ONLY);
        writeSweep = sp.getTimer(BGW_SWEEP, StatsOptions.METRICS_ONLY);
        writeWrite = sp.getTimer(BGW_WRITE, StatsOptions.METRICS_ONLY);
        writeTotal = sp.getTimer(BGW_TOTAL, StatsOptions.METRICS_ONLY);
        writeNum = sp.getHistogram(BGW_NUM, StatsOptions.METRICS_ONLY);
        numWritesRate = sp.getMeter(BGW_NUM_WRITES_RATE, StatsOptions.DEFAULT); //Enable time series

        leaseUpdate = sp.getTimer(LEASE_UPDATE, StatsOptions.METRICS_ONLY);

        mergeSuccessRetries = sp.getHistogram(MERGE_SUCCESS_NUM_RETRY, StatsOptions.METRICS_ONLY);
        mergeSuccessRate = sp.getMeter(MERGE_SUCCESS_COUNT, StatsOptions.DEFAULT); //Enable time series
        mergeSuccessTime = sp.getTimer(MERGE_SUCCESS_TIME, StatsOptions.METRICS_ONLY);
        mergeSuccessNormalizedTime = sp.getTimer(MERGE_SUCCESS_NORMALIZED_TIME, StatsOptions.METRICS_ONLY);
        mergeSuccessExclusive = sp.getMeter(MERGE_SUCCESS_EXCLUSIVE, StatsOptions.METRICS_ONLY);
        mergeSuccessSuspended = sp.getMeter(MERGE_SUCCESS_SUSPENDED, StatsOptions.METRICS_ONLY);
        mergeFailedExclusive = sp.getMeter(MERGE_FAILED_EXCLUSIVE, StatsOptions.DEFAULT); //Enable time series
        headOfQueueWaitTime = sp.getTimer(HEAD_OF_QUEUE_WAIT_TIME, StatsOptions.METRICS_ONLY);
        mergeSuspendTime = sp.getTimer(MERGE_SUSPEND_TIME, StatsOptions.METRICS_ONLY);
        mergeLockTime = sp.getTimer(MERGE_LOCK_TIME, StatsOptions.METRICS_ONLY);
        mergeCommitHookTime = sp.getTimer(MERGE_COMMIT_HOOK_TIME, StatsOptions.METRICS_ONLY);
        mergeChangesApplied = sp.getTimer(MERGE_CHANGES_APPLIED_TIME, StatsOptions.METRICS_ONLY);
        mergeChangesRate = sp.getMeter(MERGE_CHANGES_RATE, StatsOptions.METRICS_ONLY);

        branchCommitRate = sp.getMeter(BRANCH_COMMIT_COUNT, StatsOptions.DEFAULT);
        mergeBranchCommitRate = sp.getMeter(MERGE_BRANCH_COMMIT_COUNT, StatsOptions.DEFAULT);
    }

    @Override
    public void doneBackgroundRead(BackgroundReadStats stats) {
        readHead.update(stats.readHead, TimeUnit.MILLISECONDS);
        readCacheInvalidate.update(stats.cacheInvalidationTime, TimeUnit.MILLISECONDS);
        readDiffCache.update(stats.populateDiffCache, TimeUnit.MILLISECONDS);
        readLock.update(stats.lock, TimeUnit.MILLISECONDS);
        readDispatch.update(stats.dispatchChanges, TimeUnit.MILLISECONDS);
        readTotalTime.update(stats.totalReadTime, TimeUnit.MILLISECONDS);

        //Record rate of num of external changes pulled per second
        numChangesRate.mark(stats.numExternalChanges);
        numChangesHisto.update(stats.numExternalChanges);

        // update lag of external changes
        changesLag.mark(stats.externalChangesLag);
    }

    @Override
    public void doneBackgroundUpdate(BackgroundWriteStats stats) {
        writeClean.update(stats.clean, TimeUnit.MILLISECONDS);
        writeSplit.update(stats.split, TimeUnit.MILLISECONDS);
        writeSweep.update(stats.sweep, TimeUnit.MILLISECONDS);
        writeWrite.update(stats.write, TimeUnit.MILLISECONDS);
        writeTotal.update(stats.totalWriteTime, TimeUnit.MILLISECONDS);

        writeNum.update(stats.num);

        //Record rate of num of bg writes pushed per second
        numWritesRate.mark(stats.num);
    }

    @Override
    public void doneLeaseUpdate(long timeMicros) {
        leaseUpdate.update(timeMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public void doneBranchCommit() {
        branchCommitRate.mark();
    }

    @Override
    public void doneMergeBranch(int numCommits, int numChanges) {
        mergeBranchCommitRate.mark(numCommits);
        mergeChangesRate.mark(numChanges);
    }

    @Override
    public void doneMerge(int numChanges,
                          int numRetries,
                          long timeMillis,
                          long suspendMillis,
                          boolean exclusive) {
        mergeSuccessRate.mark();
        mergeSuccessRetries.update(numRetries);
        mergeSuccessTime.update(timeMillis, TimeUnit.MILLISECONDS);

        if (numChanges > 0) {
            mergeSuccessNormalizedTime.update(timeMillis / numChanges, TimeUnit.MILLISECONDS);
            mergeChangesRate.mark(numChanges);
        }

        if (exclusive) {
            mergeSuccessExclusive.mark();
        }

        mergeSuspendTime.update(suspendMillis, TimeUnit.MILLISECONDS);
        if (suspendMillis > 0) {
            mergeSuccessSuspended.mark();
        }
    }

    @Override
    public void failedMerge(int numRetries, long timeMillis, long suspendMillis, boolean exclusive) {
        if (exclusive){
            mergeFailedExclusive.mark();
        }
    }

    @Override
    public void doneWaitUntilHead(long waitMicros) {
        headOfQueueWaitTime.update(waitMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public void doneMergeLockAcquired(long timeMicros) {
        mergeLockTime.update(timeMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public void doneCommitHookProcessed(long timeMicros) {
        mergeCommitHookTime.update(timeMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public void doneChangesApplied(long timeMicros) {
        mergeChangesApplied.update(timeMicros, TimeUnit.MICROSECONDS);
    }
}
