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

import org.jetbrains.annotations.NotNull;

/**
 * Instances of {@code SegmentReader} are responsible for reading records from segments.
 * <p>
 * Passing a record id that cannot be resolved to any of the read methods will eventually
 * result in a {@link SegmentNotFoundException}. Implementations are however free to choose
 * to defer such an exception. For example by returning cached data or a thunk to a specific
 * record such that the exception is only thrown when actually accessing the returned record.
 * <p>
 * The behaviour of the read methods is implementation specific when passing a record id
 * that does not match the type of the expected record.
 */
public interface SegmentReader {

    /**
     * Read the string identified by {@code id}.
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    String readString(@NotNull RecordId id);

    /**
     * Read the map identified by {@code id}.
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    MapRecord readMap(@NotNull RecordId id);

    /**
     * Read the template identified by {@code id}.
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    Template readTemplate(@NotNull RecordId id);

    /**
     * Read the node identified by {@code id}.
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    SegmentNodeState readNode(@NotNull RecordId id);

    /**
     * Read the current head state based on the head of {@code revisions}
     * @param revisions
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    SegmentNodeState readHeadState(@NotNull Revisions revisions);

    /**
     * Read the property identified by {@code id} and {@code template}
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    SegmentPropertyState readProperty(
            @NotNull RecordId id,
            @NotNull PropertyTemplate template);

    /**
     * Read the blob identified by {@code id}.
     * @throws SegmentNotFoundException  see class comment for exception semantics
     */
    @NotNull
    SegmentBlob readBlob(@NotNull RecordId id);
}
