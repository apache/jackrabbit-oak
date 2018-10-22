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
package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SegmentArchiveManager provides a low-level access to the segment files (eg.
 * stored in the .tar). It allows to perform a few FS-like operations (delete,
 * rename, copy, etc.) and also opens the segment archives either for reading
 * or reading and writing.
 * <p>
 * The implementation doesn't need to be thread-safe.
 */
public interface SegmentArchiveManager {

    /**
     * List names of the available .tar archives.
     *
     * @return archive list
     */
    @NotNull
    List<String> listArchives() throws IOException;

    /**
     * Opens a given archive for reading.
     *
     * @param archiveName
     * @return the archive reader or null if the archive doesn't exist or doesn't
     * have a valid index
     */
    @Nullable
    SegmentArchiveReader open(@NotNull String archiveName) throws IOException;

    /**
     * Opens an archive that wasn't closed correctly.
     *
     * @param archiveName
     * @return the archive reader or null if the implementation doesn't support
     * opening an unclosed archive
     */
    @Nullable
    SegmentArchiveReader forceOpen(String archiveName) throws IOException;

    /**
     * Creates a new archive.
     *
     * @param archiveName
     * @return the archive writer
     */
    @NotNull
    SegmentArchiveWriter create(@NotNull String archiveName) throws IOException;

    /**
     * Deletes the archive if exists.
     *
     * @param archiveName
     * @return true if the archive was removed, false otherwise
     */
    boolean delete(@NotNull String archiveName);

    /**
     * Renames the archive.
     *
     * @param from the existing archive
     * @param to new name
     * @return true if the archive was renamed, false otherwise
     */
    boolean renameTo(@NotNull String from, @NotNull String to);

    /**
     * Copies the archive with all the segments.
     *
     * @param from the existing archive
     * @param to new name
     */
    void copyFile(@NotNull String from, @NotNull String to) throws IOException;

    /**
     * Check if archive exists.
     *
     * @param archiveName archive to check
     * @return true if archive exists, false otherwise
     */
    boolean exists(@NotNull String archiveName);

    /**
     * Finds all the segments included in the archive.
     *
     * @param archiveName archive to recover
     * @param entries results will be put there, in the order of presence in the
     *                archive
     */
    void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException;

}
