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
package org.apache.jackrabbit.api;

import javax.jcr.AccessDeniedException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.version.VersionException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * The Jackrabbit Node interface. This interface contains the
 * Jackrabbit-specific extensions to the JCR {@link javax.jcr.Node} interface.
 */
@ProviderType
public interface JackrabbitNode extends Node {

    /**
     * 
     * @param newName
     * @throws javax.jcr.RepositoryException
     */
    void rename(String newName) throws RepositoryException;

    /**
     *
     * @param mixinNames
     * @throws NoSuchNodeTypeException
     * @throws VersionException
     * @throws ConstraintViolationException
     * @throws LockException
     * @throws RepositoryException
     */
    void setMixins(String[] mixinNames)
            throws NoSuchNodeTypeException, VersionException,
            ConstraintViolationException, LockException, RepositoryException;

    /**
     * Returns the node at {@code relPath} relative to {@code this} node or {@code null} if no such node exists. 
     * The same reacquisition semantics apply as with {@link #getNode(String)}.
     *
     * @param relPath The relative path of the node to retrieve.
     * @return The node at {@code relPath} or {@code null}
     * @throws RepositoryException If an error occurs.
     */
    default @Nullable JackrabbitNode getNodeOrNull(@NotNull String relPath) throws RepositoryException {
        if (hasNode(relPath)) {
            Node n = getNode(relPath);
            return (n instanceof JackrabbitNode) ? (JackrabbitNode) n : null;
        } else {
            return null;
        }
    }

    /**
     * Returns the property at {@code relPath} relative to {@code this} node or {@code null} if no such property exists. 
     * The same reacquisition semantics apply as with {@link #getNode(String)}.
     *
     * @param relPath The relative path of the property to retrieve.
     * @return The property at {@code relPath} or {@code null}
     * @throws RepositoryException If an error occurs.
     */
    default @Nullable Property getPropertyOrNull(@NotNull String relPath) throws RepositoryException {
        if (hasProperty(relPath)) {
            return getProperty(relPath);
        } else {
            return null;
        }
    }

    /**
     * Same as {@link #getParent()}, but instead of throwing an ItemNotFoundException or AccessDeniedException 
     * just return {@code null}.
     * @return the parent node, or {@code null} if there is no parent or the parent node is not accessible.
     * @throws RepositoryException if an error occurs.
     */
    default @Nullable Node getParentOrNull() throws RepositoryException {
        try {
            return getParent();
        } catch (ItemNotFoundException | AccessDeniedException e) {
            return null;
        }
    }
}
