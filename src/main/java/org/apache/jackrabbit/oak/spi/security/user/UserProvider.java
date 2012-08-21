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
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CoreValue;
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
    Tree getAuthorizable(String authorizableId, int authorizableType);

    @CheckForNull
    Tree getAuthorizableByPath(String authorizableOakPath);

    @CheckForNull
    Tree getAuthorizableByPrincipal(Principal principal);

    @Nonnull
    String getAuthorizableId(Tree authorizableTree);

    boolean isAdminUser(Tree userTree);

    void setProtectedProperty(Tree authorizableTree, String propertyName, String value, int type);

    void setProtectedProperty(Tree v, String propertyName, String[] values, int type);

}