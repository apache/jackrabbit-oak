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
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public interface SegmentWriter {

    void flush() throws IOException;

    /**
     * Write a map record.
     *
     * @param base    base map relative to which the {@code changes} are applied
     *                ot {@code null} for the empty map.
     * @param changes the changed mapping to apply to the {@code base} map.
     * @return the map record written
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
     * @return The segment blob written
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
     * @return block record identifier
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeBlock(@Nonnull byte[] bytes, int offset, int length) throws IOException;

    /**
     * Writes a stream value record. The given stream is consumed <em>and
     * closed</em> by this method.
     *
     * @param stream stream to be written
     * @return blob for the passed {@code stream}
     * @throws IOException if the input stream could not be read or the output
     *                     could not be written
     */
    @Nonnull
    RecordId writeStream(@Nonnull InputStream stream) throws IOException;

    /**
     * Write a property.
     *
     * @param state the property to write
     * @return the property state written
     * @throws IOException
     */
    @Nonnull
    // TODO frm this method is only used from test code, should it be removed?
    RecordId writeProperty(@Nonnull PropertyState state) throws IOException;

    /**
     * Write a node state.
     * <p>
     * <em>Note:</em> the returned {@code SegmentNodeState} instance is bound to
     * this {@code SegmentWriter} instance. That is, future calls to {@code
     * #builder()} return a {@code NodeBuilder} that is also bound to the same
     * {@code SegmentWriter} instance and uses it for writing any changes. This
     * might not always be desired and callers of this method need to take care
     * not to proliferate this writer through the returned node states beyond
     * the intended bounds.
     *
     * @param state node state to write
     * @return segment node state equal to {@code state}
     * @throws IOException
     */
    @Nonnull
    RecordId writeNode(@Nonnull NodeState state) throws IOException;

    /**
     * Write a node state, unless cancelled.
     * <p>
     * <em>Note:</em> the returned {@code SegmentNodeState} instance is bound to
     * this {@code SegmentWriter} instance. That is, future calls to {@code
     * #builder()} return a {@code NodeBuilder} that is also bound to the same
     * {@code SegmentWriter} instance and uses it for writing any changes. This
     * might not always be desired and callers of this method need to take care
     * not to proliferate this writer through the returned node states beyond
     * the intended bounds.
     *
     * @param state  node state to write
     * @param cancel supplier to signal cancellation of this write operation
     * @return segment node state equal to {@code state} or {@code null} if
     * cancelled.
     * @throws IOException
     */
    @CheckForNull
    RecordId writeNode(@Nonnull NodeState state, @Nonnull Supplier<Boolean> cancel) throws IOException;

}
