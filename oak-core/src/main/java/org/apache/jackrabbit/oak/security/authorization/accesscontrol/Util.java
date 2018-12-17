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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation specific access control utility methods
 */
final class Util implements AccessControlConstants {

    /**
     *  Private constructor to avoid instantiation
     */
    private Util() {}

    static void checkValidPrincipal(@Nullable Principal principal,
                                    @NotNull PrincipalManager principalManager) throws AccessControlException {
        checkValidPrincipal(principal, principalManager, ImportBehavior.ABORT);
    }

    static boolean checkValidPrincipal(@Nullable Principal principal,
                                       @NotNull PrincipalManager principalManager,
                                       int importBehavior) throws AccessControlException {
        String name = (principal == null) ? null : principal.getName();
        if (name == null || name.isEmpty()) {
            throw new AccessControlException("Invalid principal " + name);
        }

        if (importBehavior == ImportBehavior.BESTEFFORT) {
            return true;
        } else {
            if (!(principal instanceof PrincipalImpl) && !principalManager.hasPrincipal(name)) {
                switch (importBehavior) {
                    case ImportBehavior.ABORT:
                        throw new AccessControlException("Unknown principal " + name);
                    case ImportBehavior.IGNORE:
                        return false;
                    default:
                        throw new IllegalArgumentException("Invalid import behavior " + importBehavior);
                }
            }
            return true;
        }
    }

    static void checkValidPrincipals(@Nullable Set<Principal> principals,
                                     @NotNull PrincipalManager principalManager) throws AccessControlException {
        if (principals == null) {
            throw new AccessControlException("Valid principals expected. Found null.");
        }
        for (Principal principal : principals) {
            checkValidPrincipal(principal, principalManager);
        }
    }

    static boolean isValidPolicy(@Nullable String oakPath, @NotNull AccessControlPolicy policy) {
        if (policy instanceof ACL) {
            String path = ((ACL) policy).getOakPath();
            return !((path == null && oakPath != null) || (path != null && !path.equals(oakPath)));
        }
        return false;
    }

    static void checkValidPolicy(@Nullable String oakPath, @NotNull AccessControlPolicy policy) throws AccessControlException {
        if (!isValidPolicy(oakPath, policy)) {
            throw new AccessControlException("Invalid access control policy " + policy);
        }
    }

    static boolean isAccessControlled(@Nullable String oakPath, @NotNull Tree tree,
                                      @NotNull ReadOnlyNodeTypeManager ntMgr) {
        String mixinName = getMixinName(oakPath);
        return ntMgr.isNodeType(tree, mixinName);
    }

    static boolean isACE(@NotNull Tree tree, @NotNull ReadOnlyNodeTypeManager ntMgr) {
        return tree.exists() && ntMgr.isNodeType(tree, NT_REP_ACE);
    }

    @NotNull
    static String getMixinName(@Nullable String oakPath) {
        return (oakPath == null) ? MIX_REP_REPO_ACCESS_CONTROLLABLE : MIX_REP_ACCESS_CONTROLLABLE;
    }

    @NotNull
    static String getAclName(@Nullable String oakPath) {
        return (oakPath == null) ? REP_REPO_POLICY : REP_POLICY;
    }

    /**
     * Create a valid name for the ACE node based on the entry and it's index.
     *
     * @param ace The access control entry.
     * @param index The index of the entry in the list
     * @return the name of the ACE node.
     */
    @NotNull
    static String generateAceName(@NotNull ACE ace, int index) {
        String hint = (ace.isAllow()) ? "allow" : "deny";
        if (index == 0) {
            return hint;
        } else {
            return hint + index;
        }
    }

    static int getImportBehavior(AuthorizationConfiguration config) {
        String importBehaviorStr = config.getParameters().getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT);
        return ImportBehavior.valueFromString(importBehaviorStr);
    }
}
