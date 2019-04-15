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

import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
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
        return Collections2.filter(Collections2.transform(privilegeNames, privilegeName -> {
            try {
                return privilegeManager.getPrivilege(namePathMapper.getJcrName(privilegeName));
            } catch (RepositoryException e) {
                log.error("Unknown privilege in access control entry : {}", privilegeName);
                return null;
            }
        }), Predicates.notNull()).toArray(new Privilege[0]);
    }

    public static boolean hasModAcPermission(@NotNull PermissionProvider permissionProvider, @NotNull String effectivePath) {
        if (REPOSITORY_PERMISSION_PATH.equals(effectivePath)) {
            return permissionProvider.getRepositoryPermission().isGranted(Permissions.MODIFY_ACCESS_CONTROL);
        } else {
            return permissionProvider.isGranted(effectivePath, Permissions.getString(Permissions.MODIFY_ACCESS_CONTROL));
        }
    }
}