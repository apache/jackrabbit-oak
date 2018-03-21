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

/**
 * Allows to write in the new archive.
 */
public interface SegmentArchiveWriter {

    /**
     * Write the new segment to the archive.
     *
     * @param msb
     * @param lsb
     * @param data
     * @param offset
     * @param size
     * @param generation
     * @param fullGeneration
     * @param isCompacted
     * @return the entry representing the new segment. Can be later used for the {@link #readSegment(long, long)} method.
     */
    @Nonnull
    void writeSegment(long msb, long lsb, @Nonnull byte[] data, int offset, int size, int generation, int fullGeneration, boolean isCompacted) throws IOException;

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
