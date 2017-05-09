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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * Implementation specific access control utility methods
 */
final class Util implements AccessControlConstants {

    /**
     *  Private constructor to avoid instantiation
     */
    private Util() {}

    public static void checkValidPrincipal(@Nullable Principal principal,
                                           @Nonnull PrincipalManager principalManager) throws AccessControlException {
        checkValidPrincipal(principal, principalManager, ImportBehavior.ABORT);
    }

    public static boolean checkValidPrincipal(@Nullable Principal principal,
                                              @Nonnull PrincipalManager principalManager,
                                              int importBehavior) throws AccessControlException {
        String name = (principal == null) ? null : principal.getName();
        if (name == null || name.isEmpty()) {
            throw new AccessControlException("Invalid principal " + name);
        }
        if (!(principal instanceof PrincipalImpl) && !principalManager.hasPrincipal(name)) {
            switch (importBehavior) {
                case ImportBehavior.ABORT:
                    throw new AccessControlException("Unknown principal " + name);
                case ImportBehavior.IGNORE:
                    return false;
                case ImportBehavior.BESTEFFORT:
                    return true;
                default: throw new IllegalArgumentException("Invalid import behavior " + importBehavior);
            }
        }
        return true;
    }

    public static void checkValidPrincipals(@Nullable Set<Principal> principals,
                                            @Nonnull PrincipalManager principalManager) throws AccessControlException {
        if (principals == null) {
            throw new AccessControlException("Valid principals expected. Found null.");
        }
        for (Principal principal : principals) {
            checkValidPrincipal(principal, principalManager);
        }
    }

    public static boolean isValidPolicy(@Nullable String oakPath, @Nonnull AccessControlPolicy policy) {
        if (policy instanceof ACL) {
            String path = ((ACL) policy).getOakPath();
            return !((path == null && oakPath != null) || (path != null && !path.equals(oakPath)));
        }
        return false;
    }

    public static void checkValidPolicy(@Nullable String oakPath, @Nonnull AccessControlPolicy policy) throws AccessControlException {
        if (!isValidPolicy(oakPath, policy)) {
            throw new AccessControlException("Invalid access control policy " + policy);
        }
    }

    public static boolean isAccessControlled(@Nullable String oakPath, @Nonnull Tree tree,
                                             @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        String mixinName = getMixinName(oakPath);
        return ntMgr.isNodeType(tree, mixinName);
    }

    public static boolean isACE(@Nonnull Tree tree, @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        return tree.exists() && ntMgr.isNodeType(tree, NT_REP_ACE);
    }

    @Nonnull
    public static String getMixinName(@Nullable String oakPath) {
        return (oakPath == null) ? MIX_REP_REPO_ACCESS_CONTROLLABLE : MIX_REP_ACCESS_CONTROLLABLE;
    }

    @Nonnull
    public static String getAclName(@Nullable String oakPath) {
        return (oakPath == null) ? REP_REPO_POLICY : REP_POLICY;
    }

    /**
     * Create a valid name for the ACE node based on the entry and it's index.
     *
     * @param ace The access control entry.
     * @param index The index of the entry in the list
     * @return the name of the ACE node.
     */
    @Nonnull
    public static String generateAceName(@Nonnull ACE ace, int index) {
        String hint = (ace.isAllow()) ? "allow" : "deny";
        if (index == 0) {
            return hint;
        } else {
            return hint + index;
        }
    }

    public static int getImportBehavior(AuthorizationConfiguration config) {
        String importBehaviorStr = config.getParameters().getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT);
        return ImportBehavior.valueFromString(importBehaviorStr);
    }
}