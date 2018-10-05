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
package org.apache.jackrabbit.oak.cache;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;

public class CacheStatsMBeanWrapper implements CacheStatsMBean {

    private final CacheStatsMBean delegate;

    public CacheStatsMBeanWrapper(CacheStatsMBean delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public long getRequestCount() {
        return delegate.getRequestCount();
    }

    @Override
    public long getHitCount() {
        return delegate.getHitCount();
    }

    @Override
    public double getHitRate() {
        return delegate.getHitRate();
    }

    @Override
    public long getMissCount() {
        return delegate.getMissCount();
    }

    @Override
    public double getMissRate() {
        return delegate.getMissRate();
    }

    @Override
    public long getLoadCount() {
        return delegate.getLoadCount();
    }

    @Override
    public long getLoadSuccessCount() {
        return delegate.getLoadSuccessCount();
    }

    @Override
    public long getLoadExceptionCount() {
        return delegate.getLoadExceptionCount();
    }

    @Override
    public double getLoadExceptionRate() {
        return delegate.getLoadExceptionRate();
    }

    @Override
    public long getTotalLoadTime() {
        return delegate.getTotalLoadTime();
    }

    @Override
    public double getAverageLoadPenalty() {
        return delegate.getAverageLoadPenalty();
    }

    @Override
    public long getEvictionCount() {
        return delegate.getEvictionCount();
    }

    @Override
    public long getElementCount() {
        return delegate.getElementCount();
    }

    @Override
    public long getMaxTotalWeight() {
        return delegate.getMaxTotalWeight();
    }

    @Override
    public long estimateCurrentWeight() {
        return delegate.estimateCurrentWeight();
    }

    @Override
    public String cacheInfoAsString() {
        return delegate.cacheInfoAsString();
    }

    @Override
    public void resetStats() {
        delegate.resetStats();
    }
}
