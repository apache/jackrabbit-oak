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
package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.file.tar.BinaryReferencesRecovery;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.jetbrains.annotations.NotNull;

/**
 * Provides the binary references index of archives migrated from a persistence, for archives
 * that have none there. The references are recovered with {@link BinaryReferencesRecovery}
 * through a {@link ReadOnlyFileStore} on the persistence, opened on first use. It must be
 * read-only: a writable store would recover the archives and modify the persistence.
 */
public class MigratedArchiveBinaryReferences implements Closeable {

    private final SegmentNodeStorePersistence persistence;

    private File directory;

    private ReadOnlyFileStore store;

    public MigratedArchiveBinaryReferences(@NotNull SegmentNodeStorePersistence persistence) {
        this.persistence = persistence;
    }

    /**
     * @param segments the segments of an archive, by segment id
     * @return the binary references index of the archive
     */
    public byte @NotNull [] recover(@NotNull Map<UUID, ByteBuffer> segments) throws IOException {
        BinaryReferencesIndexWriter index = newBinaryReferencesIndexWriter();
        BinaryReferencesRecovery recovery = new BinaryReferencesRecovery(store().recovery,
                (generation, segmentId, reference) -> index.addEntry(generation.getGeneration(),
                        generation.getFullGeneration(), generation.isCompacted(), segmentId, reference));
        for (Map.Entry<UUID, ByteBuffer> segment : segments.entrySet()) {
            recovery.recover(segment.getKey(), segment.getValue());
        }
        return index.write();
    }

    private ReadOnlyFileStore store() throws IOException {
        if (store == null) {
            directory = Files.createTempDirectory("migrated-archive-binary-references").toFile();
            try {
                store = fileStoreBuilder(directory).withCustomPersistence(persistence).buildReadOnly();
            } catch (InvalidFileStoreVersionException e) {
                throw new IOException(e);
            }
        }
        return store;
    }

    @Override
    public void close() {
        if (store != null) {
            store.close();
        }
        FileUtils.deleteQuietly(directory);
    }
}
