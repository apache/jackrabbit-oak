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

package org.apache.jackrabbit.oak.plugins.segment.compaction;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;

@Deprecated
public class DefaultCompactionStrategyMBean
        extends AnnotatedStandardMBean
        implements CompactionStrategyMBean {

    private final CompactionStrategy strategy;

    @Deprecated
    public DefaultCompactionStrategyMBean(CompactionStrategy strategy) {
        super(CompactionStrategyMBean.class);
        this.strategy = strategy;
    }

    @Override
    @Deprecated
    public boolean isCloneBinaries() {
        return strategy.cloneBinaries();
    }

    @Override
    @Deprecated
    public void setCloneBinaries(boolean cloneBinaries) {
        strategy.setCloneBinaries(cloneBinaries);
    }

    @Override
    @Deprecated
    public boolean isPausedCompaction() {
        return strategy.isPaused();
    }

    @Override
    @Deprecated
    public void setPausedCompaction(boolean pausedCompaction) {
        strategy.setPaused(pausedCompaction);
    }

    @Override
    @Deprecated
    public String getCleanupStrategy() {
        return strategy.getCleanupType();
    }

    @Override
    @Deprecated
    public void setCleanupStrategy(String cleanup) {
        strategy.setCleanupType(CleanupType.valueOf(cleanup));
    }

    @Override
    @Deprecated
    public long getOlderThan() {
        return strategy.getOlderThan();
    }

    @Override
    @Deprecated
    public void setOlderThan(long olderThan) {
        strategy.setOlderThan(olderThan);
    }

    @Override
    @Deprecated
    public byte getMemoryThreshold() {
        return strategy.getMemoryThreshold();
    }

    @Override
    @Deprecated
    public void setMemoryThreshold(byte memory) {
        strategy.setMemoryThreshold(memory);
    }

    @Override
    @Deprecated
    public boolean getForceAfterFail() {
        return strategy.getForceAfterFail();
    }

    @Override
    @Deprecated
    public void setForceAfterFail(boolean forceAfterFail) {
        strategy.setForceAfterFail(forceAfterFail);
    }

    @Override
    @Deprecated
    public int getRetryCount() {
        return strategy.getRetryCount();
    }

    @Override
    @Deprecated
    public void setRetryCount(int retryCount) {
        strategy.setRetryCount(retryCount);
    }

    @Override
    @Deprecated
    public byte getGainThreshold() {
        return strategy.getGainThreshold();
    }

    @Override
    @Deprecated
    public void setGainThreshold(byte gainThreshold) {
        strategy.setGainThreshold(gainThreshold);
    }

}
