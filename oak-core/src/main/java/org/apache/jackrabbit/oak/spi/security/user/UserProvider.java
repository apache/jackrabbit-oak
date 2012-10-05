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
package org.apache.jackrabbit.oak.spi.security.user;

import java.security.Principal;
import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * UserProvider deals with with creating and resolving repository content
 * associated with {@code User}s and {@code Group}s.
 */
public interface UserProvider {

    @Nonnull
    Tree createUser(String userId, String intermediateJcrPath) throws RepositoryException;

    @Nonnull
    Tree createGroup(String groupId, String intermediateJcrPath) throws RepositoryException;

    @CheckForNull
    Tree getAuthorizable(String authorizableId);

    @CheckForNull
    Tree getAuthorizable(String authorizableId, Type authorizableType);

    @CheckForNull
    Tree getAuthorizableByPath(String authorizableOakPath);

    @CheckForNull
    Tree getAuthorizableByPrincipal(Principal principal);

    @CheckForNull
    String getAuthorizableId(Tree authorizableTree);

    /**
     * Find the authorizable trees matching the following search parameters within
     * the sub-tree defined by an authorizable tree:
     *
     * @param propertyRelPaths An array of property names or relative paths
     * pointing to properties within the tree defined by a given authorizable node.
     * @param value The property value to look for.
     * @param ntNames An array of node type names to restrict the search within
     * the authorizable tree to a subset of nodes that match any of the node
     * type names; {@code null} indicates that no filtering by node type is
     * desired. Specifying a node type name that defines an authorizable node
     * )e.g. {@link UserConstants#NT_REP_USER rep:User} will limit the search to
     * properties defined with the authorizable node itself instead of searching
     * the complete sub-tree.
     * @param exact A boolean flag indicating if the value must match exactly or not.s
     * @param maxSize The maximal number of search results to look for.
     * @param authorizableType Filter the search results to only return authorizable
     * trees of a given type. Passing {@link Type#AUTHORIZABLE} indicates that
     * no filtering for a specific authorizable type is desired. However, properties
     * might still be search in the complete sub-tree of authorizables depending
     * on the other query parameters.
     * @return An iterator of authorizable trees that match the specified
     * search parameters and filters or an empty iterator if no result can be
     * found.
     */
    @Nonnull
    Iterator<Tree> findAuthorizables(String[] propertyRelPaths, String value, String[] ntNames, boolean exact, long maxSize, Type authorizableType);

    boolean isAuthorizableType(Tree authorizableTree, Type authorizableType);

    boolean isAdminUser(Tree userTree);

    /**
     * Returns the password hash for the user with the specified ID or {@code null}
     * if the user does not exist or if the hash is not accessible for the editing
     * session.
     *
     * @param userID The id of a user.
     * @return the password hash or {@code null}.
     */
    String getPassword(String userID);

    /**
     * Set the password for the user identified by the specified {@code userTree}.
     *
     * @param userTree The tree representing the user.
     * @param password The plaintext password to set.
     * @param forceHash If true the specified password needs to be hashed irrespective
     * of it's format.
     * @throws javax.jcr.RepositoryException If an error occurs
     */
    void setPassword(Tree userTree, String password, boolean forceHash) throws RepositoryException;

    void setProtectedProperty(Tree authorizableTree, String propertyName, String value, int propertyType);

    void setProtectedProperty(Tree v, String propertyName, String[] values, int propertyType);

}