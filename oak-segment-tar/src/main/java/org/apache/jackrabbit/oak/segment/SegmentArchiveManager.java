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
package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarEntry;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndex;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * SegmentArchiveManager provides a low-level access to the segment files (eg.
 * stored in the .tar). It allows to perform a few FS-like operations (delete,
 * rename, copy, etc.) and also opens the segment archives either for reading
 * or reading and writing.
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

    /**
     * Allows to write in the new archive.
     */
    interface SegmentArchiveWriter {

        /**
         * Write the new segment to the archive.
         *
         * @param msb
         * @param lsb
         * @param data
         * @param offset
         * @param size
         * @param generation
         * @return the entry representing the new segment. Can be later used for the {@link #readSegment(TarEntry)} method.
         */
        @Nonnull
        TarEntry writeSegment(long msb, long lsb, @Nonnull byte[] data, int offset, int size, @Nonnull GCGeneration generation) throws IOException;

        /**
         * Read the segment.
         *
         * @param tarEntry
         * @return byte buffer containing the segment data or null if segment doesn't exist
         */
        @Nullable
        ByteBuffer readSegment(@Nonnull TarEntry tarEntry) throws IOException;

        /**
         * Write the index data.
         *
         * @param data
         */
        void writeIndex(@Nonnull byte[] data) throws IOException;

        /**
         * Write the graph data.
         *
         * @param data
         */
        void writeGraph(@Nonnull byte[] data) throws IOException;

        /**
         * Write the binary references data.
         *
         * @param data
         */
        void writeBinaryReferences(@Nonnull byte[] data) throws IOException;

        /**
         * Get the current length of the archive.
         *
         * @return length of the archive, in bytes
         */
        long getLength();

        /**
         * Close the archive.
         */
        void close() throws IOException;

        /**
         * Check if the archive has been created (eg. something has been written).
         *
         * @return true if the archive has been created, false otherwise
         */
        boolean isCreated();

        /**
         * Flush all the data to the storage.
         */
        void flush() throws IOException;

        /**
         * Get the name of the archive.
         *
         * @return archive name
         */
        @Nonnull
        String getName();
    }

    interface SegmentArchiveReader {

        /**
         * Read the segment.
         *
         * @param msb
         * @param lsb
         * @return byte buffer containing the segment data or null if segment doesn't exist
         */
        @Nullable
        ByteBuffer readSegment(long msb, long lsb) throws IOException;

        /**
         * Returns the index.
         *
         * @return segment index
         */
        @Nonnull
        Index getIndex();

        /**
         * Loads and returns the graph.
         *
         * @return the segment graph or null if the persisted graph doesn't exist.
         */
        @Nullable
        Map<UUID, List<UUID>> getGraph() throws IOException;

        /**
         * Check if the persisted graph exists.
         *
         * @return true if the graph exists, false otherwise
         */
        boolean hasGraph();

        /**
         * Loads and returns the binary references.
         *
         * @return binary references
         */
        @Nonnull
        BinaryReferencesIndex getBinaryReferences() throws IOException, InvalidBinaryReferencesIndexException;

        /**
         * Get the current length of the archive.
         *
         * @return length of the archive, in bytes
         */
        long length();

        /**
         * Get the name of the archive.
         *
         * @return archive name
         */
        @Nonnull
        String getName();

        /**
         * Close the archive.
         */
        void close() throws IOException;

        /**
         * Returns the size of the entry
         * @param size
         * @return
         */
        int getEntrySize(int size);
    }

}
