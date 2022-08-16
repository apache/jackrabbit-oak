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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

final class Utils {

    private Utils() {}

    /**
     * Create the tree at the specified relative path including all missing
     * intermediate trees using the specified {@code primaryTypeName}. This
     * method treats ".." parent element and "." as current element and
     * resolves them accordingly; in case of a relative path containing parent
     * elements this may lead to tree creating outside the tree structure
     * defined by the given {@code Tree}.
     *
     * @param relativePath    A relative OAK path that may contain parent and
     *                        current elements.
     * @param primaryTypeName A oak name of a primary node type that is used
     *                        to create the missing trees.
     * @return The node util of the tree at the specified {@code relativePath}.
     * @throws AccessDeniedException If the any intermediate tree does not exist
     *                               and cannot be created.
     */
    @NotNull
    static Tree getOrAddTree(@NotNull Tree tree, @NotNull String relativePath, @NotNull String primaryTypeName) throws AccessDeniedException {
        if (PathUtils.denotesCurrent(relativePath)) {
            return tree;
        } else if (PathUtils.denotesParent(relativePath)) {
            return tree.getParent();
        } else if (relativePath.indexOf('/') == -1) {
            return TreeUtil.getOrAddChild(tree, relativePath, primaryTypeName);
        } else {
            for (String element : PathUtils.elements(relativePath)) {
                if (PathUtils.denotesParent(element)) {
                    tree = tree.getParent();
                } else if (!PathUtils.denotesCurrent(element)) {
                    tree = TreeUtil.getOrAddChild(tree, element, primaryTypeName);
                }  // else . -> skip to next element
            }
            return tree;
        }
    }

    static boolean canHavePasswordExpired(@NotNull String userId, @NotNull ConfigurationParameters config) {
        return !UserUtil.isAdmin(config, userId) || config.getConfigValue(UserAuthentication.PARAM_PASSWORD_EXPIRY_FOR_ADMIN, false);
    }

    static boolean canHavePasswordExpired(@NotNull User user, @NotNull ConfigurationParameters config) {
        return !user.isAdmin() || config.getConfigValue(UserAuthentication.PARAM_PASSWORD_EXPIRY_FOR_ADMIN, false);
    }
    
    static boolean isEveryone(@NotNull Authorizable authorizable) {
        return authorizable.isGroup() && EveryonePrincipal.NAME.equals(getPrincipalName(authorizable));
    }
    
    @Nullable
    private static String getPrincipalName(@NotNull Authorizable authorizable) {
        if (authorizable instanceof AuthorizableImpl) {
            return ((AuthorizableImpl) authorizable).getPrincipalNameOrNull();
        } else {
            try {
                return authorizable.getPrincipal().getName();
            } catch (RepositoryException e) {
                return null;
            }
        }
    }
    
    @Nullable
    static String getIdOrNull(@NotNull Authorizable authorizable) {
        try {
            return authorizable.getID();
        } catch (RepositoryException e) {
            return null;
        }
    }
    
    @NotNull
    static Tree getTree(@NotNull Authorizable authorizable, @NotNull Root root) throws RepositoryException {
        if (authorizable instanceof TreeAware) {
            return ((TreeAware) authorizable).getTree();
        } else {
            return root.getTree(authorizable.getPath());
        }
    }
}
