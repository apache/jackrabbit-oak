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

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_IMPERSONATOR_PRINCIPAL_NAMES;

public final class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private Utils() {
    }

    /**
     * Create the tree at the specified relative path including all missing intermediate trees using
     * the specified {@code primaryTypeName}. This method treats ".." parent element and "." as
     * current element and resolves them accordingly; in case of a relative path containing parent
     * elements this may lead to tree creating outside the tree structure defined by the given
     * {@code Tree}.
     *
     * @param relativePath    A relative OAK path that may contain parent and current elements.
     * @param primaryTypeName An oak name of a primary node type that is used to create the missing
     *                        trees.
     * @return The node util of the tree at the specified {@code relativePath}.
     * @throws AccessDeniedException If the intermediate tree does not exist and cannot be created.
     */
    @NotNull
    static Tree getOrAddTree(@NotNull Tree tree, @NotNull String relativePath,
        @NotNull String primaryTypeName) throws AccessDeniedException {
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

    static boolean canHavePasswordExpired(@NotNull String userId,
        @NotNull ConfigurationParameters config) {
        return !UserUtil.isAdmin(config, userId) || config.getConfigValue(
            UserAuthentication.PARAM_PASSWORD_EXPIRY_FOR_ADMIN, false);
    }

    static boolean canHavePasswordExpired(@NotNull User user,
        @NotNull ConfigurationParameters config) {
        return !user.isAdmin() || config.getConfigValue(
            UserAuthentication.PARAM_PASSWORD_EXPIRY_FOR_ADMIN, false);
    }

    static boolean isEveryone(@NotNull Authorizable authorizable) {
        return authorizable.isGroup() && EveryonePrincipal.NAME.equals(
            getPrincipalName(authorizable));
    }

    /**
     * Return {@code true} if the given principal can impersonate all users. The implementation
     * tests if the given principal refers to an existing {@code User} for which
     * {@link User#isAdmin()} returns {@code true} OR if the user's principal name or any of its
     * membership is configured to impersonate all users.
     *
     * @param principal   A non-null principal instance.
     * @param userManager The user manager used for the lookup calling
     *                    {@link UserManager#getAuthorizable(Principal))}
     * @return {@code true} if the given principal can impersonate all users; {@code false} if that
     * condition is not met or if the evaluation failed.
     */
    public static boolean canImpersonateAllUsers(@NotNull Principal principal,
        @NotNull UserManager userManager) {
        try {
            Authorizable authorizable = userManager.getAuthorizable(principal);
            if (authorizable == null || authorizable.isGroup()) {
                return false;
            }

            User user = (User) authorizable;
            return user.isAdmin() || Utils.isImpersonator(user, userManager);
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return false;
        }
    }

    /**
     * Return {@code true} if the given user has the right to impersonate. The implementation tests
     * if the given user refers to an existing {@code Principal} that is either member of a
     * configured impersonator group or is has its name amongst configured impersonators. Both those
     * configurations are under the {@code PARAM_IMPERSONATOR_PRINCIPAL_NAMES} configuration value.
     *
     * @param user        A non-null user instance.
     * @param userManager The user manager implementation to retrieve the configuration and
     *                    principal manager.
     * @return {@code true} if the given user is an impersonator; {@code false} if that condition is
     * not met or if the evaluation failed.
     */
    private static boolean isImpersonator(@NotNull User user, @NotNull UserManager userManager)
        throws RepositoryException {
        if (!(userManager instanceof UserManagerImpl)) {
            return false;
        }
        UserManagerImpl umImpl = (UserManagerImpl) userManager;
        Set<String> impersonatorPrincipals = Set.of(umImpl.getConfig().getConfigValue(
            PARAM_IMPERSONATOR_PRINCIPAL_NAMES,
            new String[]{}));
        if (impersonatorPrincipals.isEmpty()) {
            return false;
        }

        Principal userPrincipal = user.getPrincipal();
        PrincipalManager principalManager = umImpl.getPrincipalManager();
        for (String impersonatorPrincipalName : impersonatorPrincipals) {
            Principal impersonatorPrincipal = principalManager.getPrincipal(
                impersonatorPrincipalName);
            if (impersonatorPrincipal == null) {
                continue;
            }

            if (GroupPrincipals.isGroup(impersonatorPrincipal)) {
                if (((GroupPrincipal) impersonatorPrincipal).isMember(userPrincipal)) {
                    return true;
                }
            } else if (impersonatorPrincipalName.equals(userPrincipal.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@code true} if the given principal is admin. The implementation tests if the given
     * principal refers to an existing {@code User} for which {@link User#isAdmin()} returns
     * {@code true}.
     *
     * @param principal   A non-null principal instance.
     * @param userManager The user manager used for the lookup calling
     *                    {@link UserManager#getAuthorizable(Principal))}
     * @return {@code true} if the given principal is admin; {@code false} if that condition is not
     * met or if the evaluation failed.
     */
    public static boolean isAdmin(@NotNull Principal principal, @NotNull UserManager userManager) {
        try {
            Authorizable authorizable = userManager.getAuthorizable(principal);
            if (authorizable == null || authorizable.isGroup()) {
                return false;
            }

            return ((User) authorizable).isAdmin();
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return false;
        }
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
    static Tree getTree(@NotNull Authorizable authorizable, @NotNull Root root)
        throws RepositoryException {
        if (authorizable instanceof TreeAware) {
            return ((TreeAware) authorizable).getTree();
        } else {
            return root.getTree(authorizable.getPath());
        }
    }
}
