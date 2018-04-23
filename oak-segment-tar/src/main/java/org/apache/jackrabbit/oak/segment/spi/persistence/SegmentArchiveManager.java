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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

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
     * List names of the available archives.
     *
     * @return archive list
     */
    @Nonnull
    List<String> listArchives() throws IOException;

    /**
     * Opens a given archive for reading.
     *
     * @param archiveName
     * @return the archive reader or null if the archive doesn't exist
     */
    @Nullable
    SegmentArchiveReader open(@Nonnull String archiveName) throws IOException;

    /**
     * Creates a new archive.
     *
     * @param archiveName
     * @return the archive writer
     */
    @Nonnull
    SegmentArchiveWriter create(@Nonnull String archiveName) throws IOException;

    /**
     * Deletes the archive if exists.
     *
     * @param archiveName
     * @return true if the archive was removed, false otherwise
     */
    boolean delete(@Nonnull String archiveName);

    /**
     * Renames the archive.
     *
     * @param from the existing archive
     * @param to new name
     * @return true if the archive was renamed, false otherwise
     */
    boolean renameTo(@Nonnull String from, @Nonnull String to);

    /**
     * Copies the archive with all the segments.
     *
     * @param from the existing archive
     * @param to new name
     */
    void copyFile(@Nonnull String from, @Nonnull String to) throws IOException;

    /**
     * Check if archive exists.
     *
     * @param archiveName archive to check
     * @return true if archive exists, false otherwise
     */
    boolean exists(@Nonnull String archiveName);

    /**
     * Finds all the segments included in the archive.
     *
     * @param archiveName archive to recover
     * @param entries results will be put there, in the order of presence in the
     *                archive
     */
    void recoverEntries(@Nonnull String archiveName, @Nonnull LinkedHashMap<UUID, byte[]> entries) throws IOException;

}
