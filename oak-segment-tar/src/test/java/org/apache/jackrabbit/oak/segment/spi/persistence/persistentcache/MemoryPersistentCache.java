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
 *
 */
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

class MemoryPersistentCache extends AbstractPersistentCache {

    private final Map<String, Buffer> segments = new ConcurrentHashMap<>();

    private boolean throwException = false;

    public MemoryPersistentCache(boolean throwException) {
        this.throwException = throwException;
        segmentCacheStats = new SegmentCacheStats(
                "Memory Cache",
                () -> null,
                () -> null,
                () -> null,
                () -> null,
                () -> 0L);
    }

    @Override
    protected Buffer readSegmentInternal(long msb, long lsb) {
        return segments.get(String.valueOf(msb) + lsb);
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return segments.containsKey(String.valueOf(msb) + lsb);
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer) {
        segments.put(String.valueOf(msb) + lsb, buffer);
    }

    @Override
    public Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) throws RepositoryNotReachableException {
        return super.readSegment(msb, lsb, () -> {
            if (throwException) {
                throw new RepositoryNotReachableException(null);
            }
            return loader.call();
        });
    }

    @Override
    public void cleanUp() {

    }
}