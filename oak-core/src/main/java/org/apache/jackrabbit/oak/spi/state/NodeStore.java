/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

/**
 * Storage abstraction for trees. At any given point in time the stored
 * tree is rooted at a single immutable node state.
 * <p>
 * This is a low-level interface that doesn't cover functionality like
 * merging concurrent changes or rejecting new tree states based on some
 * higher-level consistency constraints.
 */
public interface NodeStore {

    /**
     * Returns the latest state of the tree.
     *
     * @return root node state
     */
    @Nonnull
    NodeState getRoot();

    /**
     * Merges the changes from the passed {@code builder} into
     * the store.
     *
     * @param builder  the builder whose changes to apply
     * @param commitHook the commit hook to apply while merging changes
     * @param info commit info associated with this merge operation
     * @return the node state resulting from the merge.
     * @throws CommitFailedException if the merge failed
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    @Nonnull
    NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nonnull CommitInfo info) throws CommitFailedException;

    /**
     * Rebase the changes in the passed {@code builder} on top of the current root state.
     *
     * @param builder  the builder to rebase
     * @return the node state resulting from the rebase.
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    @Nonnull
    NodeState rebase(@Nonnull NodeBuilder builder);

    /**
     * Reset the passed {@code builder} by throwing away all its changes and
     * setting its base state to the current root state.
     *
     * @param builder the builder to reset
     * @return the node state resulting from the reset.
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    NodeState reset(@Nonnull NodeBuilder builder);

    /**
     * Create a {@link Blob} from the given input stream. The input stream
     * is closed after this method returns.
     * @param inputStream  The input stream for the {@code Blob}
     * @return  The {@code Blob} representing {@code inputStream}
     * @throws IOException  If an error occurs while reading from the stream
     */
    @Nonnull
    Blob createBlob(InputStream inputStream) throws IOException;

    /**
     * Get a blob by its reference.
     * @param reference  reference to the blob
     * @return  blob or {@code null} if the reference does not resolve to a blob.
     * @see Blob#getReference()
     */
    @CheckForNull
    Blob getBlob(@Nonnull String reference);

    /**
     * Creates a new checkpoint of the latest root of the tree. The checkpoint
     * remains valid for at least as long as requested and allows that state
     * of the repository to be retrieved using the returned opaque string
     * reference.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @return string reference of this checkpoint
     */
    @Nonnull
    String checkpoint(long lifetime);

    /**
     * Retrieves the root node from a previously created repository checkpoint.
     *
     * @param checkpoint string reference of a checkpoint
     * @return the root node of the checkpoint,
     *         or {@code null} if the checkpoint is no longer available
     */
    @CheckForNull
    NodeState retrieve(@Nonnull String checkpoint);

    /**
     * Releases the provided checkpoint. If the provided checkpoint doesn't exist this method should return {@code true}.
     *
     * @param checkpoint string reference of a checkpoint
     * @return {@code true} if the checkpoint was successfully removed, or if it doesn't exist
     */
    boolean release(@Nonnull String checkpoint);

}
