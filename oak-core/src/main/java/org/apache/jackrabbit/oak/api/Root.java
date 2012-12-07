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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * The root of a {@link Tree}.
 * <p>
 * The data returned by this class filtered for the access rights that are set
 * in the {@link ContentSession} that created this object.
 * <p>
 * All root instances created by a content session become invalid after the
 * content session is closed. Any method called on an invalid root instance
 * will throw an {@code InvalidStateException}.
 */
public interface Root {

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
     * Copy the child located at {@code sourcePath} to a child at {@code destPath}.
     * Both paths must be absolute and resolve to a child located in this root.<br>
     *
     * This method does nothing an returns {@code false} if
     * <ul>
     *     <li>The tree at {@code sourcePath} does exist or is not accessible,</li>
     *     <li>the parent of the tree at {@code destinationPath} does not exist or is not accessible,</li>
     *     <li>a tree already exists at {@code destinationPath}.</li>
     * </ul>
     * If a tree at {@code destinationPath} exists but is not accessible to the
     * editing content session this method succeeds but a subsequent
     * {@link #commit()} will detect the violation and fail.
     *
     * @param sourcePath source path
     * @param destPath destination path
     * @return  {@code true} on success, {@code false} otherwise.
     */
    boolean copy(String sourcePath, String destPath);

    /**
     * Retrieve the {@code Tree} at the given absolute {@code path}. The path
     * must resolve to a tree in this root.
     *
     * @param path absolute path to the tree
     * @return tree at the given path or {@code null} if no such tree exists or
     *         if the tree at {@code path} is not accessible.
     */
    @CheckForNull
    Tree getTree(String path);

    /**
     * Get a tree location for from {@code pathResolver}
     *
     * @param pathResolver for the path to the location
     * @return the tree location for {@code pathResolver}
     */
    @Nonnull
    TreeLocation getLocation(PathResolver pathResolver);

    /**
     * Rebase this root instance to the latest revision. After a call to this method,
     * all trees obtained through {@link #getTree(String)} become invalid and fresh
     * instances must be obtained.
     */
    void rebase();

    /**
     * Reverts all changes made to this root and refreshed to the latest trunk.
     * After a call to this method, all trees obtained through {@link #getTree(String)}
     * become invalid and fresh instances must be obtained.
     */
    void refresh();

    /**
     * Atomically apply all changes made to the tree beneath this root to the
     * underlying store and refreshes this root. After a call to this method,
     * all trees obtained through {@link #getTree(String)} become invalid and fresh
     * instances must be obtained.
     *
     * @throws CommitFailedException
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
    SessionQueryEngine getQueryEngine();

    /**
     * Returns the blob factory (TODO: review if that really belongs to the OAK-API. see also todos on BlobFactory)
     *
     * @return the blob factory.
     */
    @Nonnull
    BlobFactory getBlobFactory();
}
