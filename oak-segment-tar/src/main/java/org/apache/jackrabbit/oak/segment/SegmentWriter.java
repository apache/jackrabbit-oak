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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Converts nodes, properties, values, etc. to records and persists them.
 */
public interface SegmentWriter {

    void flush() throws IOException;

    /**
     * Write a blob (as list of block records)
     *
     * @param blob blob to write
     * @return the record id of the blob written
     * @throws IOException
     */
    @NotNull
    RecordId writeBlob(@NotNull Blob blob) throws IOException;

    /**
     * Writes a stream value record. The given stream is consumed <em>and
     * closed</em> by this method.
     *
     * @param stream stream to be written
     * @return the record id of the stream written
     * @throws IOException if the input stream could not be read or the output
     *                     could not be written
     */
    @NotNull
    RecordId writeStream(@NotNull InputStream stream) throws IOException;

    /**
     * Write a node state. If non null, the passed {@code stableId} will be assigned to
     * the persisted node. Otherwise the stable id will be inferred from {@code state}.
     *
     * @param state node state to write
     * @param stableIdBytes the stableId that should be assigned to the node or {@code null}.
     * @return the record id of the segment node state written
     * @throws IOException
     */
    @NotNull
    RecordId writeNode(@NotNull NodeState state, @Nullable Buffer stableIdBytes) throws IOException;

    /**
     * Write a node state.
     * <p>
     * Equivalent to {@code writeNode(state, null)}
     *
     * @see #writeNode(NodeState, Buffer)
     */
    @NotNull
    default RecordId writeNode(@NotNull NodeState state) throws IOException {
        return writeNode(state, null);
    }

}
