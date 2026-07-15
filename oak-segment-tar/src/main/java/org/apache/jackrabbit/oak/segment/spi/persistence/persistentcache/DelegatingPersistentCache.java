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
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Callable;

/**
 * Simple abstract implementation of a delegating PersistentCache that can be
 * used as a base class for {@link PersistentCache} decorators.
 */
public abstract class DelegatingPersistentCache implements PersistentCache {

    protected abstract PersistentCache delegate();

    @Override
    public @Nullable Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
        return delegate().readSegment(msb, lsb, loader);
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return delegate().containsSegment(msb, lsb);
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer) {
        delegate().writeSegment(msb, lsb, buffer);
    }

    @Override
    public void cleanUp() {
        delegate().cleanUp();
    }

    @Override
    public AbstractCacheStats getCacheStats() {
        return delegate().getCacheStats();
    }
}
