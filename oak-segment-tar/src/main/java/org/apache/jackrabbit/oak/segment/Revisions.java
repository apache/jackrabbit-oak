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

import javax.annotation.Nonnull;

import com.google.common.base.Function;

/**
 * {@code Revisions} instances provide read and write access to
 * the current head state. Implementations are thread safe
 * and all setters act atomically.
 * <p>
 * This is a low level API and it is the callers and implementors
 * responsibility to ensure all record id passed to or returned
 * from methods of this interface are the ids of node states.
 */
public interface Revisions {
    /**
     * Implementation specific options for the {@code setHead} methods.
     * These options can e.g. be used to specify priority, timeout, etc.
     * for individual method calls.
     */
    interface Option {}

    /**
     * Returns the record id of the head state. The returned id
     * is a valid id for a {@code SegmentNodeState}.
     * @return  id of the head state
     */
    @Nonnull
    RecordId getHead();
    
    /**
     * Returns the <b>persisted</b> to disk record id of the head state. 
     * The returned id is a valid id for a {@code SegmentNodeState}.
     * @return  id of the head state
     */
    @Nonnull
    RecordId getPersistedHead();

    /**
     * Atomically set the record id of the current head state to the
     * given {@code head} state if the current head state matches
     * the {@code expected} value.
     * <p>
     * All record ids must be valid ids for {@code SegmentNodeState}s.
     * <p>
     * The locking behaviour of this method regarding implementation
     * specific.
     *
     * @param expected  the expected head for the update to take place
     * @param head      the new head to update to
     * @param options   implementation specific options
     * @return          {@code true} if the current head was successfully
     *                  updated, {@code false} otherwise.
     */
    boolean setHead(@Nonnull RecordId expected,
                    @Nonnull RecordId head,
                    @Nonnull Option... options);

    /**
     * Atomically set the record id of the current head state to the value
     * returned from the {@code newHead} function when called with the record
     * id of the current head.
     * <p>
     * All record ids must be valid ids for {@code SegmentNodeState}s.
     * <p>
     * The behaviour of this method regarding locking and handling
     * {@code null} values returned by {@code newHead} is implementation specific.
     *
     * @param newHead  function mapping an record id to the record id to which
     *                 the current head id should be set.
     * @param options  implementation specific options
     * @return         the record id of the root node if the current head was successfully
     *                 updated, {@code null} otherwise.
     * @throws InterruptedException
     *                 Blocking implementations may throw this exception whe
     *                 interrupted.
     */
    RecordId setHead(@Nonnull Function<RecordId, RecordId> newHead,
                    @Nonnull Option... options)
    throws InterruptedException;
}


