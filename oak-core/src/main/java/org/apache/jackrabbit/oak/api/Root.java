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

/**
 * The root of a {@link Tree}.
 */
public interface Root {

    /**
     * Move the child located at {@code sourcePath} to a child
     * at {@code destPath}. Do nothing if either the source
     * does not exist, the parent of the destination does not exist
     * or the destination exists already. Both paths must resolve
     * to a child located beneath this root.
     *
     * @param sourcePath source path relative to this root
     * @param destPath destination path relative to this root
     * @return  {@code true} on success, {@code false} otherwise.
     */
    boolean move(String sourcePath, String destPath);

    /**
     * Copy the child located at {@code sourcePath} to a child
     * at {@code destPath}. Do nothing if either the source
     * does not exist, the parent of the destination does not exist
     * or the destination exists already. Both paths must resolve
     * to a child located in this root.
     *
     * @param sourcePath source path relative to this root
     * @param destPath destination path relative to this root
     * @return  {@code true} on success, {@code false} otherwise.
     */
    boolean copy(String sourcePath, String destPath);

    /**
     * Retrieve the {@code Tree} at the given {@code path}. The path must resolve to
     * a tree in this root.
     *
     * @param path  path to the tree
     * @return  tree at the given path or {@code null} if no such tree exists
     */
    Tree getTree(String path);

    /**
     * Rebase this root to the latest revision.  After a call to this method,
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
     * @throws CommitFailedException TODO: add description and clarify how JCR exception can be generated from this generic exception
     */
    void commit() throws CommitFailedException;

    /**
     * Determine whether there are changes on this tree
     * @return  {@code true} iff this tree was modified
     */
    boolean hasPendingChanges();
}
