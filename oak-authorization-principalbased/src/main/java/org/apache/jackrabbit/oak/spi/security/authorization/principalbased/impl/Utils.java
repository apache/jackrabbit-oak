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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

final class Utils implements Constants {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private Utils() {}

    /**
     * Returns {@code true} if the given tree exists and represents a valid principal policy node, i.e. name equals to
     * {@link #REP_PRINCIPAL_POLICY} and primary type name equals to {@link #NT_REP_PRINCIPAL_POLICY}. Otherwise this
     * method returns {@code false}.
     *
     * @param tree The tree to be tested.
     * @return {@code true} if the given tree exists and represents a valid principal policy node, i.e. name equals to
     * {@link #REP_PRINCIPAL_POLICY} and primary type name equals to {@link #NT_REP_PRINCIPAL_POLICY}; otherwise
     * returns {@code false}.
     */
    public static boolean isPrincipalPolicyTree(@NotNull Tree tree) {
        return tree.exists() && REP_PRINCIPAL_POLICY.equals(tree.getName()) && NT_REP_PRINCIPAL_POLICY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    public static boolean isPrincipalEntry(@NotNull Tree tree) {
        return NT_REP_PRINCIPAL_ENTRY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    /**
     * Validate the specified {@code principal} taking the configured
     * {@link ImportBehavior} into account.
     *
     * @param principal The principal to validate.
     * @return if the principal can be handled by the filter
     * @throws AccessControlException If the principal has an invalid name or
     * if {@link ImportBehavior#ABORT} is configured and this principal cannot be handled by the filter.
     */
    public static boolean canHandle(@NotNull Principal principal, @NotNull Filter filter, int importBehavior) throws AccessControlException {
        String name = principal.getName();
        if (Strings.isNullOrEmpty(name)) {
            throw new AccessControlException("Invalid principal " + name);
        }

        boolean canHandle = filter.canHandle(Collections.singleton(principal));
        switch (importBehavior) {
            case ImportBehavior.ABORT:
                if (!canHandle) {
                    throw new AccessControlException("Unsupported principal " + name);
                }
                break;
            case ImportBehavior.IGNORE:
            case ImportBehavior.BESTEFFORT:
                log.debug("Ignoring unsupported principal {}", name);
                break;
            default:
                throw new IllegalArgumentException("Unsupported import behavior " + importBehavior);
        }
        return canHandle;
    }

    /**
     * Returns an array of privileges from the given Oak names. Note that {@link RepositoryException} thrown by
     * {@link PrivilegeManager#getPrivilege(String)} will be swallowed but notified in the error log.
     *
     * @param privilegeNames The Oak names of privileges as stored in the repository.
     * @param privilegeManager The {@link PrivilegeManager} to retrieve the privileges.
     * @param namePathMapper The {@link NamePathMapper} to convert the Oak names to JCR names.
     * @return An array of {@link Privilege} for the given names.
     */
    public static Privilege[] privilegesFromOakNames(@NotNull Set<String> privilegeNames, @NotNull PrivilegeManager privilegeManager, @NotNull NamePathMapper namePathMapper) {
        return privilegeNames.stream().map(privilegeName -> {
            try {
                return privilegeManager.getPrivilege(namePathMapper.getJcrName(privilegeName));
            } catch (RepositoryException e) {
                log.error("Unknown privilege in access control entry : {}", privilegeName);
                return null;
            }
        }).filter(Objects::nonNull).toArray(Privilege[]::new);
    }

    public static boolean hasModAcPermission(@NotNull PermissionProvider permissionProvider, @NotNull String effectivePath) {
        if (REPOSITORY_PERMISSION_PATH.equals(effectivePath)) {
            return permissionProvider.getRepositoryPermission().isGranted(Permissions.MODIFY_ACCESS_CONTROL);
        } else {
            return permissionProvider.isGranted(effectivePath, Permissions.getString(Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    /**
     * Tests if the given ACE tree comes with any restrictions. Since node type {@code rep:PrincipalEntry} requires 
     * restrictions to be stored in a separate {@code rep:restrictions} child node, this can be determined by checking if
     * a restriction child-node exists.
     * 
     * @param aceTree The tree defining the principal-based access control entry
     * @return {@code true} if the given ACE defines restrictions, {@code false} otherwise.
     */
    public static boolean hasRestrictions(@NotNull Tree aceTree) {
        return aceTree.hasChild(REP_RESTRICTIONS);
    }
    
    public static boolean hasValidRestrictions(@Nullable String oakPath, @NotNull Tree aceTree, @NotNull RestrictionProvider restrictionProvider) {
        if (hasRestrictions(aceTree)) {
            try {
                restrictionProvider.validateRestrictions(oakPath, aceTree);
                return true;
            } catch (RepositoryException e) {
                log.warn("Access control entry at {} contains unsupported restrictions: {}", oakPath, e.getMessage());
                return false;
            } 
        } else {
            // no restriction tree present -> skip validation as principal-acl does not allow for restriction properties 
            // on the ACE-node itself as the regular rep:policy in the default access control management. 
            return true;
        }
    }

    /**
     * Utility method that conditionally reads restrictions from provider if the given {@code aceTree} has restriction 
     * child tree, i.e. combining {@link #hasRestrictions(Tree)} with {@link RestrictionProvider#readRestrictions(String, Tree)}.
     * 
     * @param provider The restriction provider
     * @param effectivePath The effective path
     * @param aceTree The ace tree
     * @return restrictions as read from the provider if the given {@code aceTree} has a rep:restriction child. otherwise,
     * an empty set without calling the provider.
     */
    public static Set<Restriction> readRestrictions(@NotNull RestrictionProvider provider, @Nullable String effectivePath, @NotNull Tree aceTree) {
        if (hasRestrictions(aceTree)) {
            return provider.readRestrictions(effectivePath, aceTree);
        } else {
            return Collections.emptySet();
        }
    }
}