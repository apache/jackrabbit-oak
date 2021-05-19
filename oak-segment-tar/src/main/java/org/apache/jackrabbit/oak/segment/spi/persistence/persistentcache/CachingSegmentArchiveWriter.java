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
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class CachingSegmentArchiveWriter implements SegmentArchiveWriter {

    @NotNull
    private final PersistentCache persistentCache;

    @NotNull
    private final SegmentArchiveWriter delegate;

    public CachingSegmentArchiveWriter(@NotNull PersistentCache persistentCache, @NotNull SegmentArchiveWriter delegate) {
        this.persistentCache = persistentCache;
        this.delegate = delegate;
    }

    @Override
    @NotNull
    public void writeSegment(long msb, long lsb, @NotNull byte[] data, int offset, int size, int generation, int fullGeneration, boolean isCompacted) throws IOException {
        delegate.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, isCompacted);
        persistentCache.writeSegment(msb, lsb, Buffer.wrap(data, offset, size));
    }

    @Override
    @Nullable
    public Buffer readSegment(long msb, long lsb) throws IOException {
        return persistentCache.readSegment(msb, lsb, () -> delegate.readSegment(msb, lsb));
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return delegate.containsSegment(msb, lsb);
    }

    @Override
    public void writeGraph(@NotNull byte[] data) throws IOException {
        delegate.writeGraph(data);
    }

    @Override
    public void writeBinaryReferences(@NotNull byte[] data) throws IOException {
        delegate.writeBinaryReferences(data);
    }

    @Override
    public long getLength() {
        return delegate.getLength();
    }

    @Override
    public int getEntryCount() {
        return delegate.getEntryCount();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean isCreated() {
        return delegate.isCreated();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    @NotNull
    public String getName() {
        return delegate.getName();
    }
}
