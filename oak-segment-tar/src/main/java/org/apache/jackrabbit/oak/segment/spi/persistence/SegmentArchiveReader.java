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
import java.nio.ByteBuffer;
import java.util.List;

public interface SegmentArchiveReader {

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
     * Check if the segment exists.
     *
     * @param msb
     * @param lsb
     * @return true if the segment exists
     */
    boolean containsSegment(long msb, long lsb);

    /**
     * List all the segments, in the order as they have been written to the archive.
     *
     * @return segment list, ordered by their position in the archive
     */
    List<SegmentArchiveEntry> listSegments();

    /**
     * Loads and returns the graph.
     *
     * @return the segment graph or null if the persisted graph doesn't exist.
     */
    @Nullable
    ByteBuffer getGraph() throws IOException;

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
    ByteBuffer getBinaryReferences() throws IOException;

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
