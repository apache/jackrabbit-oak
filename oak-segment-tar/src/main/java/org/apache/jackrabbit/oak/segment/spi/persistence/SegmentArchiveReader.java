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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This interface represents a read-only segment archive. Since the underlying
 * data structure is immutable, the implementation <b>should be</b> thread safe.
 */
public interface SegmentArchiveReader extends Closeable {

    /**
     * Read the segment.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return byte buffer containing the segment data or null if the segment doesn't exist
     */
    @Nullable
    Buffer readSegment(long msb, long lsb) throws IOException;

    /**
     * Check if the segment exists.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return {@code true} if the segment exists
     */
    boolean containsSegment(long msb, long lsb);

    /**
     * List all the segments, in the order as they have been written to the archive.
     *
     * @return segment list, ordered by their position in the archive
     */
    List<SegmentArchiveEntry> listSegments();

    /**
     * Load the segment graph.
     *
     * @return byte buffer representing the graph or null if the graph hasn't been
     * persisted.
     */
    @Nullable
    Buffer getGraph() throws IOException;

    /**
     * Check if the segment graph has been persisted for this archive.
     *
     * @return {@code true} if the graph exists, false otherwise
     */
    boolean hasGraph();

    /**
     * Load binary references.
     *
     * @return byte buffer representing the binary references structure.
     */
    @NotNull
    Buffer getBinaryReferences() throws IOException;

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
    @NotNull
    String getName();

    /**
     * Close the archive.
     */
    @Override
    void close() throws IOException;

    /**
     * Transforms the segment size in bytes into the effective size on disk for
     * the given entry (eg. by adding the number of padding bytes, header, etc.)
     *
     * @param size the segment size in bytes
     * @return the number of bytes effectively used on the storage to save the
     * segment
     */
    int getEntrySize(int size);
}
