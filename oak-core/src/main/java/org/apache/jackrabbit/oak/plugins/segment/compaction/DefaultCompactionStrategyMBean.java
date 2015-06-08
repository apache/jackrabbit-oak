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
import org.apache.jackrabbit.oak.plugins.segment.CompactionMap;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;

public class DefaultCompactionStrategyMBean
        extends AnnotatedStandardMBean
        implements CompactionStrategyMBean {

    private final CompactionStrategy strategy;

    public DefaultCompactionStrategyMBean(CompactionStrategy strategy) {
        super(CompactionStrategyMBean.class);
        this.strategy = strategy;
    }

    @Override
    public boolean isCloneBinaries() {
        return strategy.cloneBinaries();
    }

    @Override
    public void setCloneBinaries(boolean cloneBinaries) {
        strategy.setCloneBinaries(cloneBinaries);
    }

    @Override
    public boolean isPausedCompaction() {
        return strategy.isPaused();
    }

    @Override
    public void setPausedCompaction(boolean pausedCompaction) {
        strategy.setPaused(pausedCompaction);
    }

    @Override
    public String getCleanupStrategy() {
        return strategy.getCleanupType();
    }

    @Override
    public void setCleanupStrategy(String cleanup) {
        strategy.setCleanupType(CleanupType.valueOf(cleanup));
    }

    @Override
    public long getOlderThan() {
        return strategy.getOlderThan();
    }

    @Override
    public void setOlderThan(long olderThan) {
        strategy.setOlderThan(olderThan);
    }

    @Override
    public byte getMemoryThreshold() {
        return strategy.getMemoryThreshold();
    }

    @Override
    public void setMemoryThreshold(byte memory) {
        strategy.setMemoryThreshold(memory);
    }

    @Override
    public String getCompactionMapStats() {
        CompactionMap cm = strategy.getCompactionMap();
        if (cm != null) {
            return cm.getCompactionStats();
        }
        return "";
    }
}
