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
import java.util.Map;

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
     * Merges the changes between the
     * {@link NodeBuilder#getBaseState() base} and
     * {@link NodeBuilder#getNodeState() head} states
     * of the given builder to this store.
     *
     * @param builder the builder whose changes to apply
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
     * Rebases the changes between the
     * {@link NodeBuilder#getBaseState() base} and
     * {@link NodeBuilder#getNodeState() head} states
     * of the given builder on top of the current root state.
     * The base state of the given builder becomes the latest
     * {@link #getRoot() root} state of the repository, and the
     * head state will contain the rebased changes.
     *
     * @param builder the builder to rebase
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
     * <p>
     * The {@code properties} passed to this methods are associated with the
     * checkpoint and can be retrieved through the {@link #checkpointInfo(String)}
     * method. Its semantics is entirely application specific.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @param properties properties to associate with the checkpoint
     * @return string reference of this checkpoint
     */
    @Nonnull
    String checkpoint(long lifetime, @Nonnull Map<String, String> properties);

    /**
     * Creates a new checkpoint of the latest root of the tree. The checkpoint
     * remains valid for at least as long as requested and allows that state
     * of the repository to be retrieved using the returned opaque string
     * reference.
     * <p>
     * This method is a shortcut for {@link #checkpoint(long, Map)} passing
     * an empty map for its 2nd argument.
     *
     * @param lifetime time (in milliseconds, &gt; 0) that the checkpoint
     *                 should remain available
     * @return string reference of this checkpoint
     */
    @Nonnull
    String checkpoint(long lifetime);

    /**
     * Retrieve the properties associated with a checkpoint.
     *
     * @param checkpoint string reference of a checkpoint
     * @return the properties associated with the checkpoint referenced by
     *         {@code checkpoint} or an empty map when there is no such
     *         checkpoint.
     */
    @Nonnull
    Map<String, String> checkpointInfo(@Nonnull String checkpoint);

    /**
     * Returns all valid checkpoints. The returned {@code Iterable} provides a
     * snapshot of valid checkpoints at the time this method is called. That
     * is, the {@code Iterable} will not reflect checkpoints created after this
     * method was called.
     * <p>
     * See {@link #checkpoint(long, Map)} for a definition of a valid
     * checkpoint.
     *
     * @return valid checkpoints.
     */
    @Nonnull
    Iterable<String> checkpoints();

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
