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

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class CachingArchiveManager implements SegmentArchiveManager {

    private final SegmentArchiveManager delegate;

    private final PersistentCache persistentCache;

    public CachingArchiveManager(PersistentCache persistentCache, SegmentArchiveManager delegate) {
        this.delegate = delegate;
        this.persistentCache = persistentCache;
    }

    @Override
    public @NotNull List<String> listArchives() throws IOException {
        return delegate.listArchives();
    }

    @Override
    public @Nullable SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
        SegmentArchiveReader delegateArchiveReader = delegate.open(archiveName);

        if (delegateArchiveReader != null) {
            return new CachingSegmentArchiveReader(persistentCache, delegateArchiveReader);
        } else {
            return null;
        }
    }

    @Override
    public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        SegmentArchiveReader delegateArchiveReader = delegate.forceOpen(archiveName);

        if (delegateArchiveReader != null) {
            return new CachingSegmentArchiveReader(persistentCache, delegateArchiveReader);
        } else {
            return null;
        }
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
        return delegate.create(archiveName);
    }

    @Override
    public boolean delete(@NotNull String archiveName) {
        return delegate.delete(archiveName);
    }

    @Override
    public boolean renameTo(@NotNull String from, @NotNull String to) {
        return delegate.renameTo(from, to);
    }

    @Override
    public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
        delegate.copyFile(from, to);
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return delegate.exists(archiveName);
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
        delegate.recoverEntries(archiveName, entries);
    }

    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName,
            @NotNull Set<UUID> recoveredEntries) throws IOException {
        delegate.backup(archiveName, backupArchiveName, recoveredEntries);
    }
}