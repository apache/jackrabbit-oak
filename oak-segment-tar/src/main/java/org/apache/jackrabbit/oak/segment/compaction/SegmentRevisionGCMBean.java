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

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;

// FIXME OAK-4617: Align SegmentRevisionGC MBean with new generation based GC
public class SegmentRevisionGCMBean
        extends AnnotatedStandardMBean
        implements SegmentRevisionGC {

    private final SegmentGCOptions gcOptions;

    public SegmentRevisionGCMBean(SegmentGCOptions gcOptions) {
        super(SegmentRevisionGC.class);
        this.gcOptions = gcOptions;
    }

    @Override
    public boolean isPausedCompaction() {
        return gcOptions.isPaused();
    }

    @Override
    public void setPausedCompaction(boolean paused) {
        gcOptions.setPaused(paused);
    }

    @Override
    public int getGainThreshold() {
        return gcOptions.getGainThreshold();
    }

    @Override
    public void setGainThreshold(int gainThreshold) {
        gcOptions.setGainThreshold(gainThreshold);
    }

    @Override
    public int getMemoryThreshold() {
        return gcOptions.getMemoryThreshold();
    }

    @Override
    public void setMemoryThreshold(int memoryThreshold) {
        gcOptions.setMemoryThreshold(memoryThreshold);
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
    public boolean getForceAfterFail() {
        return gcOptions.getForceAfterFail();
    }

    @Override
    public void setForceAfterFail(boolean forceAfterFail) {
        gcOptions.setForceAfterFail(forceAfterFail);
    }

    @Override
    public int getLockWaitTime() {
        return gcOptions.getLockWaitTime();
    }

    @Override
    public void setLockWaitTime(int lockWaitTime) {
        gcOptions.setLockWaitTime(lockWaitTime);
    }

    @Override
    public int getRetainedGenerations() {
        return gcOptions.getRetainedGenerations();
    }

    @Override
    public void setRetainedGenerations(int retainedGenerations) {
        gcOptions.setRetainedGenerations(retainedGenerations);
    }
}
