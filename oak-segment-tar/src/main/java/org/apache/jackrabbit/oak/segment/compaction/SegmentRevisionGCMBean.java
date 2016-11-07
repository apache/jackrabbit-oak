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

package org.apache.jackrabbit.oak.segment.compaction;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;

public class SegmentRevisionGCMBean
        extends AnnotatedStandardMBean
        implements SegmentRevisionGC {

    @Nonnull
    private final FileStore fileStore;

    @Nonnull
    private final SegmentGCOptions gcOptions;

    @Nonnull
    private final FileStoreGCMonitor fileStoreGCMonitor;

    public SegmentRevisionGCMBean(
            @Nonnull FileStore fileStore,
            @Nonnull SegmentGCOptions gcOptions,
            @Nonnull FileStoreGCMonitor fileStoreGCMonitor) {
        super(SegmentRevisionGC.class);
        this.fileStore = checkNotNull(fileStore);
        this.gcOptions = checkNotNull(gcOptions);
        this.fileStoreGCMonitor = checkNotNull(fileStoreGCMonitor);
    }

    //------------------------------------------------------------< SegmentRevisionGC >---

    @Override
    public boolean isPausedCompaction() {
        return gcOptions.isPaused();
    }

    @Override
    public void setPausedCompaction(boolean paused) {
        gcOptions.setPaused(paused);
    }

    @Override
    public int getRetryCount() {
        return gcOptions.getRetryCount();
    }

    @Override
    public void setRetryCount(int retryCount) {
        gcOptions.setRetryCount(retryCount);
    }

    @Override
    public int getForceTimeout() {
        return gcOptions.getForceTimeout();
    }

    @Override
    public void setForceTimeout(int timeout) {
        gcOptions.setForceTimeout(timeout);
    }

    @Override
    public int getRetainedGenerations() {
        return gcOptions.getRetainedGenerations();
    }

    @Override
    public void setRetainedGenerations(int retainedGenerations) {
        gcOptions.setRetainedGenerations(retainedGenerations);
    }

    @Override
    public long getGcSizeDeltaEstimation() {
        return gcOptions.getGcSizeDeltaEstimation();
    }

    @Override
    public void setGcSizeDeltaEstimation(long gcSizeDeltaEstimation) {
        gcOptions.setGcSizeDeltaEstimation(gcSizeDeltaEstimation);
    }

    @Override
    public boolean isEstimationDisabled() {
        return gcOptions.isEstimationDisabled();
    }

    @Override
    public void setEstimationDisabled(boolean disabled)  {
        gcOptions.setEstimationDisabled(disabled);
    }

    @Override
    public void startRevisionGC() {
        fileStore.getGCRunner().run();
    }

    @Override
    public void cancelRevisionGC() {
        fileStore.cancelGC();
    }

    @CheckForNull
    @Override
    public String getLastCompaction() {
        return fileStoreGCMonitor.getLastCompaction();
    }

    @CheckForNull
    @Override
    public String getLastCleanup() {
        return fileStoreGCMonitor.getLastCleanup();
    }

    @Override
    public long getLastRepositorySize() {
        return fileStoreGCMonitor.getLastRepositorySize();
    }

    @Override
    public long getLastReclaimedSize() {
        return fileStoreGCMonitor.getLastReclaimedSize();
    }

    @CheckForNull
    @Override
    public String getLastError() {
        return fileStoreGCMonitor.getLastError();
    }

    @Nonnull
    @Override
    public String getStatus() {
        return fileStoreGCMonitor.getStatus();
    }

    @Override
    public int getMemoryThreshold() {
        return gcOptions.getMemoryThreshold();
    }

    @Override
    public void setMemoryThreshold(int memoryThreshold) {
        gcOptions.setMemoryThreshold(memoryThreshold);
    }
}
