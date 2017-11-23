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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.NAMES;

/**
 * Implementation of the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}
 * interface that allows to create, modify and remove closed user group policies.
 */
class CugAccessControlManager extends AbstractAccessControlManager implements CugConstants, PolicyOwner {

    private static final Logger log = LoggerFactory.getLogger(CugAccessControlManager.class);

    private final Set<String> supportedPaths;
    private final ConfigurationParameters config;
    private final PrincipalManager principalManager;

    public CugAccessControlManager(@Nonnull Root root,
                                   @Nonnull NamePathMapper namePathMapper,
                                   @Nonnull SecurityProvider securityProvider,
                                   @Nonnull Set<String> supportedPaths) {
        super(root, namePathMapper, securityProvider);

        this.supportedPaths = supportedPaths;

        config = securityProvider.getConfiguration(AuthorizationConfiguration.class).getParameters();
        principalManager = securityProvider.getConfiguration(PrincipalConfiguration.class).getPrincipalManager(root, namePathMapper);
    }

    //-----------------------------------------------< AccessControlManager >---

    @Nonnull
    @Override
    public Privilege[] getSupportedPrivileges(@Nullable String absPath) throws RepositoryException {
        if (isSupportedPath(getOakPath(absPath))) {
            return new Privilege[] {privilegeFromName(PrivilegeConstants.JCR_READ)};
        } else {
            return new Privilege[0];
        }
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        if (oakPath != null && isSupportedPath(oakPath)) {
            CugPolicy cug = getCugPolicy(oakPath);
            if (cug != null) {
                return new AccessControlPolicy[]{cug};
            }
        }
        return new AccessControlPolicy[0];
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);

        boolean enabled = config.getConfigValue(CugConstants.PARAM_CUG_ENABLED, false);
        if (enabled) {
            Root r = getRoot().getContentSession().getLatestRoot();
            List<AccessControlPolicy> effective = new ArrayList<>();
            while (oakPath != null) {
                if (isSupportedPath(oakPath)) {
                    CugPolicy cug = getCugPolicy(oakPath, r.getTree(oakPath));
                    if (cug != null) {
                        effective.add(cug);
                    }
                }
                oakPath = (PathUtils.denotesRoot(oakPath)) ? null : PathUtils.getAncestorPath(oakPath, 1);
            }
            return effective.toArray(new AccessControlPolicy[effective.size()]);
        } else {
            return new AccessControlPolicy[0];
        }
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        if (oakPath == null || !isSupportedPath(oakPath)) {
            return AccessControlPolicyIteratorAdapter.EMPTY;
        } else {
            CugPolicy cug = getCugPolicy(oakPath);
            if (cug == null) {
                cug = new CugPolicyImpl(oakPath, getNamePathMapper(), principalManager, CugUtil.getImportBehavior(config));
                return new AccessControlPolicyIteratorAdapter(ImmutableSet.of(cug));
            } else {
                return AccessControlPolicyIteratorAdapter.EMPTY;
            }
        }
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        if (isSupportedPath(oakPath)) {
            checkValidPolicy(absPath, policy);

            Tree tree = getTree(oakPath, Permissions.MODIFY_ACCESS_CONTROL, true);
            Tree cug = tree.getChild(REP_CUG_POLICY);
            if (!CugUtil.definesCug(cug)) {
                throw new AccessControlException("Unexpected primary type of node rep:cugPolicy.");
            } else {
                // remove the rep:CugMixin if it has been explicitly added upon setPolicy
                Set<String> mixins = Sets.newHashSet(TreeUtil.getNames(tree, NodeTypeConstants.JCR_MIXINTYPES));
                if (mixins.remove(MIX_REP_CUG_MIXIN)) {
                    tree.setProperty(JcrConstants.JCR_MIXINTYPES, mixins, NAMES);
                } else {
                    log.debug("Cannot remove mixin type " + MIX_REP_CUG_MIXIN);
                }
                cug.remove();
            }
        } else {
            throw new AccessControlException("Unsupported path: " + absPath);
        }
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        if (isSupportedPath(oakPath)) {
            checkValidPolicy(absPath, policy);

            Tree tree = getTree(oakPath, Permissions.MODIFY_ACCESS_CONTROL, true);
            Tree typeRoot = getRoot().getTree(NodeTypeConstants.NODE_TYPES_PATH);
            if (!TreeUtil.isNodeType(tree, MIX_REP_CUG_MIXIN, typeRoot)) {
                TreeUtil.addMixin(tree, MIX_REP_CUG_MIXIN, typeRoot, null);
            }
            Tree cug;
            if (tree.hasChild(REP_CUG_POLICY)) {
                cug = tree.getChild(REP_CUG_POLICY);
                if (!CugUtil.definesCug(cug)) {
                    throw new AccessControlException("Unexpected primary type of node rep:cugPolicy.");
                }
            } else {
                cug = TreeUtil.addChild(tree, REP_CUG_POLICY, NT_REP_CUG_POLICY, typeRoot, null);
            }
            cug.setProperty(REP_PRINCIPAL_NAMES, ((CugPolicyImpl) policy).getPrincipalNames(), Type.STRINGS);
        } else {
            throw new AccessControlException("Unsupported path: " + absPath);
        }
    }

    //-------------------------------------< JackrabbitAccessControlManager >---

    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) throws RepositoryException {
        // editing by 'principal' is not supported
        return new JackrabbitAccessControlPolicy[0];
    }

    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) throws RepositoryException {
        // editing by 'principal' is not supported
        return new JackrabbitAccessControlPolicy[0];
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(Set<Principal> principals) throws RepositoryException {
        // editing by 'principal' is not supported
        return new AccessControlPolicy[0];
    }

    //--------------------------------------------------------< PolicyOwner >---
    @Override
    public boolean defines(@Nullable String absPath, @Nonnull AccessControlPolicy accessControlPolicy) {
        return isValidPolicy(absPath, accessControlPolicy);
    }

    //--------------------------------------------------------------------------

    private boolean isSupportedPath(@Nullable String oakPath) throws RepositoryException {
        checkValidPath(oakPath);
        return CugUtil.isSupportedPath(oakPath, supportedPaths);
    }

    private void checkValidPath(@Nullable String oakPath) throws RepositoryException {
        if (oakPath != null) {
            getTree(oakPath, Permissions.NO_PERMISSION, false);
        }
    }

    @CheckForNull
    private CugPolicy getCugPolicy(@Nonnull String oakPath) throws RepositoryException {
        return getCugPolicy(oakPath, getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true));
    }

    @CheckForNull
    private CugPolicy getCugPolicy(@Nonnull String oakPath, @Nonnull Tree tree) {
        Tree cug = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cug)) {
            return new CugPolicyImpl(oakPath, getNamePathMapper(), principalManager, CugUtil.getImportBehavior(config), getPrincipals(cug));
        } else {
            return null;
        }
    }

    private Set<Principal> getPrincipals(@Nonnull Tree cugTree) {
        PropertyState property = cugTree.getProperty(REP_PRINCIPAL_NAMES);
        if (property == null) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(Iterables.transform(property.getValue(Type.STRINGS), principalName -> {
                Principal principal = principalManager.getPrincipal(principalName);
                if (principal == null) {
                    log.debug("Unknown principal " + principalName);
                    principal = new PrincipalImpl(principalName);
                }
                return principal;
            }));
        }
    }

    private static boolean isValidPolicy(@Nullable String absPath, @Nonnull AccessControlPolicy policy) {
        return policy instanceof CugPolicyImpl && ((CugPolicyImpl) policy).getPath().equals(absPath);
    }

    private static void checkValidPolicy(@Nullable String absPath, @Nonnull AccessControlPolicy policy) throws AccessControlException {
        if (!(policy instanceof CugPolicyImpl)) {
            throw new AccessControlException("Unsupported policy implementation: " + policy);
        }

        CugPolicyImpl cug = (CugPolicyImpl) policy;
        if (!cug.getPath().equals(absPath)) {
            throw new AccessControlException("Path mismatch: Expected " + cug.getPath() + ", Found: " + absPath);
        }
    }
}