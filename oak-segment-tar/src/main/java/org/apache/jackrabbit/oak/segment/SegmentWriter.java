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

package org.apache.jackrabbit.oak.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Converts nodes, properties, values, etc. to records and persists them.
 */
public interface SegmentWriter {

    void flush() throws IOException;

    /**
     * Write a map record.
     *
     * @param base    base map relative to which the {@code changes} are applied
     *                ot {@code null} for the empty map.
     * @param changes the changed mapping to apply to the {@code base} map.
     * @return the record id of the map written
     * @throws IOException
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeMap(@Nullable MapRecord base, @Nonnull Map<String, RecordId> changes) throws IOException;

    /**
     * Write a list record.
     *
     * @param list the list to write.
     * @return the record id of the list written
     * @throws IOException
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeList(@Nonnull List<RecordId> list) throws IOException;

    /**
     * Write a string record.
     *
     * @param string the string to write.
     * @return the record id of the string written.
     * @throws IOException
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeString(@Nonnull String string) throws IOException;

    /**
     * Write a blob (as list of block records)
     *
     * @param blob blob to write
     * @return the record id of the blob written
     * @throws IOException
     */
    @Nonnull
    RecordId writeBlob(@Nonnull Blob blob) throws IOException;

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes  source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return the record id of the block written
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeBlock(@Nonnull byte[] bytes, int offset, int length) throws IOException;

    /**
     * Writes a stream value record. The given stream is consumed <em>and
     * closed</em> by this method.
     *
     * @param stream stream to be written
     * @return the record id of the stream written
     * @throws IOException if the input stream could not be read or the output
     *                     could not be written
     */
    @Nonnull
    RecordId writeStream(@Nonnull InputStream stream) throws IOException;

    /**
     * Write a property.
     *
     * @param state the property to write
     * @return the record id of the property state written
     * @throws IOException
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeProperty(@Nonnull PropertyState state) throws IOException;

    /**
     * Write a node state. If non null, the passed {@code stableId} will be assigned to
     * the persisted node. Otherwise the stable id will be inferred from {@code state}.
     *
     * @param state node state to write
     * @param stableIdBytes the stableId that should be assigned to the node or {@code null}.
     * @return the record id of the segment node state written
     * @throws IOException
     */
    @Nonnull
    RecordId writeNode(@Nonnull NodeState state, @Nullable ByteBuffer stableIdBytes) throws IOException;

    /**
     * Write a node state.
     * <p>
     * Equivalent to {@code writeNode(state, null)}
     *
     * @see #writeNode(NodeState, ByteBuffer)
     */
    @Nonnull
    default RecordId writeNode(@Nonnull NodeState state) throws IOException {
        return writeNode(state, null);
    }
}
