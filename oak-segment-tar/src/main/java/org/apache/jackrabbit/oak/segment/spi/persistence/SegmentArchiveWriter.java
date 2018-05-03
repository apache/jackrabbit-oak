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
 * Represents a write-enabled, append-only archive. It allows to append segments
 * and other data structures (segment graph, serialized binary references) to the
 * archive and also to read the already persisted segments.<p>
 * Caller will use the methods modifying the archive in the following order:
 * <ol>
 *     <li>phase 1:
 *         <ul>
 *             <li>{@link #writeSegment(long, long, byte[], int, int, int, int, boolean)}</li>
 *             <li>{@link #flush()}</li>
 *         </ul>
 *         repeated in an unspecified order</li>
 *     <li>{@link #writeBinaryReferences(byte[])}</li>
 *     <li>{@link #writeGraph(byte[])} (optionally)</li>
 *     <li>{@link #close()}</li>
 * </ol>
 * All the calls above are synchronized by the caller.
 * In the first phase of the writer lifecycle, the
 * write() and the flush() will be called many times, in an unspecified order. At
 * the end of the writer life cycle, the rest of the methods (2-4) will be called.
 * <p>
 * Before the {@link #close()}, all the non-modifying methods
 * (eg. {@link #readSegment(long, long)}, {@link #getLength()}} can be invoked at
 * any time. They <b>should be thread safe</b>.
 */
public interface SegmentArchiveWriter {

    /**
     * Write the new segment to the archive.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @param data the data.
     * @param offset the start offset in the data.
     * @param size the number of bytes to write.
     * @param generation the segment generation, see {@link SegmentArchiveEntry#getGeneration()}
     * @param fullGeneration the segment full generation, see {@link SegmentArchiveEntry#getFullGeneration()}
     * @param isCompacted the segment compaction property, see {@link SegmentArchiveEntry#isCompacted()}
     * @throws IOException
     */
    @Nonnull
    void writeSegment(long msb, long lsb, @Nonnull byte[] data, int offset, int size, int generation, int fullGeneration, boolean isCompacted) throws IOException;

    /**
     * Read the segment.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return byte buffer containing the segment data or null if segment doesn't exist
     */
    @Nullable
    ByteBuffer readSegment(long msb, long lsb) throws IOException;

    /**
     * Check if the segment exists.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return true if the segment exists
     */
    boolean containsSegment(long msb, long lsb);

    /**
     * Write the graph data.
     *
     * @param data serialized segment graph data
     */
    void writeGraph(@Nonnull byte[] data) throws IOException;

    /**
     * Write the binary references data.
     *
     * @param data serialized binary references data
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
     * Flush all the data to the storage. After returning from this method
     * successfully, all the segments written with the {@link #writeSegment(long, long, byte[], int, int, int, int, boolean)}
     * should be actually saved to the storage.
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
