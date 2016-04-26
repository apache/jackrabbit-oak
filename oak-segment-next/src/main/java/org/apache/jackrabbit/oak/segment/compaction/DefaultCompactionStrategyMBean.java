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

public class DefaultCompactionStrategyMBean
        extends AnnotatedStandardMBean
        implements CompactionStrategyMBean {

    private final CompactionStrategy strategy;

    public DefaultCompactionStrategyMBean(CompactionStrategy strategy) {
        super(CompactionStrategyMBean.class);
        this.strategy = strategy;
    }

    @Override
    public boolean isPausedCompaction() {
        return strategy.isPaused();
    }

    @Override
    public void setPausedCompaction(boolean paused) {
        strategy.setPaused(paused);
    }

    @Override
    public int getGainThreshold() {
        return strategy.getGainThreshold();
    }

    @Override
    public void setGainThreshold(int gainThreshold) {
        strategy.setGainThreshold(gainThreshold);
    }

    @Override
    public int getMemoryThreshold() {
        return strategy.getMemoryThreshold();
    }

    @Override
    public void setMemoryThreshold(int memoryThreshold) {
        strategy.setMemoryThreshold(memoryThreshold);
    }

    @Override
    public int getRetryCount() {
        return strategy.getRetryCount();
    }

    @Override
    public void setRetryCount(int retryCount) {
        strategy.setRetryCount(retryCount);
    }

    @Override
    public boolean getForceAfterFail() {
        return strategy.getForceAfterFail();
    }

    @Override
    public void setForceAfterFail(boolean forceAfterFail) {
        strategy.setForceAfterFail(forceAfterFail);
    }

    @Override
    public int getLockWaitTime() {
        return strategy.getLockWaitTime();
    }

    @Override
    public void setLockWaitTime(int lockWaitTime) {
        strategy.setLockWaitTime(lockWaitTime);
    }
}
