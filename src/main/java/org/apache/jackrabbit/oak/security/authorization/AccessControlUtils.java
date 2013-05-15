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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * Access control specific utility methods
 */
public final class AccessControlUtils extends org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils implements AccessControlConstants {

    /**
     *  Private constructor to avoid instantiation
     */
    private AccessControlUtils() {}

    public static void checkValidPrincipal(Principal principal, PrincipalManager principalManager) throws AccessControlException {
        String name = (principal == null) ? null : principal.getName();
        if (name == null || name.isEmpty()) {
            throw new AccessControlException("Invalid principal " + name);
        }
        if (!(principal instanceof PrincipalImpl) && !principalManager.hasPrincipal(name)) {
            throw new AccessControlException("Unknown principal " + name);
        }
    }

    public static void checkValidPrincipals(@Nullable Set<Principal> principals, PrincipalManager principalManager) throws AccessControlException {
        if (principals == null) {
            throw new AccessControlException("Valid principals expected. Found null.");
        }
        for (Principal principal : principals) {
            AccessControlUtils.checkValidPrincipal(principal, principalManager);
        }
    }

    public static void checkValidPolicy(@Nullable String oakPath, @Nonnull AccessControlPolicy policy) throws AccessControlException {
        if (policy instanceof ACL) {
            String path = ((ACL) policy).getOakPath();
            if ((path == null && oakPath != null) || (path != null && !path.equals(oakPath))) {
                throw new AccessControlException("Invalid access control policy " + policy + ": path mismatch " + oakPath);
            }
        } else {
            throw new AccessControlException("Invalid access control policy " + policy);
        }
    }

    public static boolean isAccessControlled(String oakPath, @Nonnull Tree tree, @Nonnull ReadOnlyNodeTypeManager ntMgr) {
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
     * Create a unique valid name for the Permission nodes to be save.
     *
     * @param aclTree The acl for which a new ACE name should be generated.
     * @param isAllow If the ACE is allowing or denying.
     * @return the name of the ACE node.
     */
    @Nonnull
    public static String generateAceName(@Nonnull Tree aclTree, boolean isAllow) {
        int i = 0;
        String hint = (isAllow) ? "allow" : "deny";
        String aceName = hint;
        while (aclTree.hasChild(aceName)) {
            aceName = hint + i;
            i++;
        }
        return aceName;
    }
}