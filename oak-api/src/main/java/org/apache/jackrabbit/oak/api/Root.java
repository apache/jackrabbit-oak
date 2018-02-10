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
package org.apache.jackrabbit.oak.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A {@code Root} instance serves as a container for a {@link Tree}. It is
 * obtained from a {@link ContentSession}, which governs accessibility and
 * visibility of the {@code Tree} and its sub trees.
 * <p>
 * All root instances created by a content session become invalid after the
 * content session is closed. Any method called on an invalid root instance
 * will throw an {@code InvalidStateException}.
 * <p>
 * {@link Tree} instances may become non existing after a call to
 * {@link #refresh()}, {@link #rebase()} or {@link #commit()}.
 * Any write access to non existing {@code Tree} instances will cause an
 * {@code InvalidStateException}.
 * @see Tree Existence and iterability of trees
 */
public interface Root {
    /**
     * Name of the entry of the commit path in the {@code info}
     * map in {@link #commit(java.util.Map)}
     */
    String COMMIT_PATH = "path";

    /**
     * Move the child located at {@code sourcePath} to a child at {@code destPath}.
     * Both paths must be absolute and resolve to a child located beneath this
     * root.<br>
     *
     * This method does nothing and returns {@code false} if
     * <ul>
     *     <li>the tree at {@code sourcePath} does not exist or is not accessible,</li>
     *     <li>the parent of the tree at {@code destinationPath} does not exist or is not accessible,</li>
     *     <li>a tree already exists at {@code destinationPath}.</li>
     * </ul>
     * If a tree at {@code destinationPath} exists but is not accessible to the
     * editing content session this method succeeds but a subsequent
     * {@link #commit()} will detect the violation and fail.
     *
     * @param sourcePath The source path
     * @param destPath The destination path
     * @return {@code true} on success, {@code false} otherwise.
     */
    boolean move(String sourcePath, String destPath);

    /**
     * Retrieve the possible non existing {@code Tree} at the given absolute {@code path}.
     * The path must resolve to a tree in this root.
     *
     * @param path absolute path to the tree
     * @return tree at the given path.
     */
    @Nonnull
    Tree getTree(@Nonnull String path);

    /**
     * Rebase this root instance to the latest revision. After a call to this method,
     * trees obtained through {@link #getTree(String)} may become non existing.
     */
    void rebase();

    /**
     * Reverts all changes made to this root and refreshed to the latest trunk.
     * After a call to this method, trees obtained through {@link #getTree(String)}
     * may become non existing.
     */
    void refresh();

    /**
     * Atomically persists all changes made to the tree attached to this root.
     * <p>
     * If {@code info} contains a mapping for {@link #COMMIT_PATH} and the
     * associated value is a string, implementations may throw a
     * {@code CommitFailedException} if there are changes outside of the subtree
     * designated by that path and the implementation does not support
     * such partial commits. However all implementation must handler the
     * case where a {@code path} designates a subtree that contains all
     * unpersisted changes.
     * <p>
     * The {@code info} map is passed to the underlying storage
     * as a part of the internal commit information attached to this commit.
     * The commit information will be made available to local observers but
     * will not be visible to observers on other cluster nodes.
     * <p>
     * After a successful operation the root is automatically
     * {@link #refresh() refreshed}, such that trees previously obtained
     * through {@link #getTree(String)} may become non existing.
     *
     * @param info commit information
     * @throws CommitFailedException if the commit failed
     */
    void commit(@Nonnull Map<String, Object> info) throws CommitFailedException;

    /**
     * Atomically persists all changes made to the tree attached to this root.
     * Calling this method is equivalent to calling the
     * {@link #commit(Map info)} method with an empty info map.
     *
     * @throws CommitFailedException if the commit failed
     */
    void commit() throws CommitFailedException;

    /**
     * Determine whether there are changes on this tree
     * @return  {@code true} iff this tree was modified
     */
    boolean hasPendingChanges();

    /**
     * Get the query engine.
     * 
     * @return the query engine
     */
    @Nonnull
    QueryEngine getQueryEngine();

    /**
     * Reads (and closes) the given stream and returns a {@link Blob} that
     * contains that binary. The returned blob will remain valid at least
     * until the {@link ContentSession} of this root is closed, or longer
     * if it has been committed as a part of a content update.
     * <p>
     * The implementation may decide to persist the blob at any point
     * during or between this method method call and a {@link #commit()}
     * that includes the blob, but the blob will become visible to other
     * sessions only after such a commit.
     *
     * @param stream the stream for reading the binary
     * @return the blob that was created
     * @throws IOException if the stream could not be read
     */
    @Nonnull
    Blob createBlob(@Nonnull InputStream stream) throws IOException;

    /**
     * Get a blob by its reference.
     * @param reference  reference to the blob
     * @return  blob or {@code null} if the reference does not resolve to a blob.
     * @see Blob#getReference()
     */
    @CheckForNull
    Blob getBlob(@Nonnull String reference);

    /**
     * Get the {@code ContentSession} from which this root was acquired
     * 
     * @return the associated ContentSession
     * 
     * @throws UnsupportedOperationException
     */
    @Nonnull
    ContentSession getContentSession();

}
