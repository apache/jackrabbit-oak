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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

/**
 * Implementation of the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}
 * interface that allows to create, modify and remove closed user group policies.
 */
class CugAccessControlManager extends AbstractAccessControlManager implements CugConstants, PolicyOwner {

    private static final Logger log = LoggerFactory.getLogger(CugAccessControlManager.class);

    private final Set<String> supportedPaths;
    private final CugExclude cugExclude;
    private final ConfigurationParameters config;
    private final PrincipalManager principalManager;
    private final RootProvider rootProvider;

    CugAccessControlManager(@NotNull Root root,
                            @NotNull NamePathMapper namePathMapper,
                            @NotNull SecurityProvider securityProvider,
                            @NotNull Set<String> supportedPaths,
                            @NotNull CugExclude cugExclude, RootProvider rootProvider) {
        super(root, namePathMapper, securityProvider);

        this.supportedPaths = supportedPaths;
        this.cugExclude = cugExclude;

        config = securityProvider.getConfiguration(AuthorizationConfiguration.class).getParameters();
        principalManager = securityProvider.getConfiguration(PrincipalConfiguration.class).getPrincipalManager(root, namePathMapper);
        this.rootProvider = rootProvider;
    }

    //-----------------------------------------------< AccessControlManager >---

    @NotNull
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
            Root r = getLatestRoot();
            List<AccessControlPolicy> effective = new ArrayList<>();
            collectEffectiveCugs(oakPath, r, effective, new HashSet<>());
            return effective.toArray(new AccessControlPolicy[0]);
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
                cug = new CugPolicyImpl(oakPath, getNamePathMapper(), principalManager, CugUtil.getImportBehavior(config), cugExclude);
                return new AccessControlPolicyIteratorAdapter(Set.of(cug));
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
                Set<String> mixins = CollectionUtils.toSet(TreeUtil.getNames(tree, JCR_MIXINTYPES));
                if (mixins.remove(MIX_REP_CUG_MIXIN)) {
                    tree.setProperty(JCR_MIXINTYPES, mixins, NAMES);
                } else {
                    log.debug("Cannot remove mixin type {}", MIX_REP_CUG_MIXIN);
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

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(@NotNull Principal principal) {
        // editing by 'principal' is not supported
        return new JackrabbitAccessControlPolicy[0];
    }

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(@NotNull Principal principal) {
        // editing by 'principal' is not supported
        return new JackrabbitAccessControlPolicy[0];
    }

    @NotNull
    @Override
    public AccessControlPolicy[] getEffectivePolicies(@NotNull Set<Principal> principals) {
        if (!config.getConfigValue(CugConstants.PARAM_CUG_ENABLED, false) || principals.isEmpty()) {
            return new AccessControlPolicy[0];
        }
        Root r = getLatestRoot();
        Set<String> candidates = collectEffectiveCandidates(r, Iterables.transform(principals, Principal::getName));
        if (candidates.isEmpty()) {
            return new AccessControlPolicy[0];
        } else {
            List<AccessControlPolicy> effective = new ArrayList<>();
            for (String path : candidates) {
                Tree t = r.getTree(path);
                if (t.exists()) {
                    CugPolicy cug = getCugPolicy(path, t, true);
                    if (cug != null) {
                        effective.add(cug);
                    }
                }
            }
            return effective.toArray(new AccessControlPolicy[0]);
        }
    }

    @Override
    public @NotNull Iterator<AccessControlPolicy> getEffectivePolicies(@NotNull Set<Principal> principals, @Nullable String... absPaths) throws RepositoryException {
        if (principals.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (absPaths == null || absPaths.length == 0) {
            return Iterators.forArray(getEffectivePolicies(principals));
        }

        boolean enabled = config.getConfigValue(CugConstants.PARAM_CUG_ENABLED, false);
        if (enabled) {
            Root r = getLatestRoot();
            List<AccessControlPolicy> effective = new ArrayList<>();
            Set<String> visitedPaths = new HashSet<>();
            for (String jcrPath : absPaths) {
                String oakPath = getOakPath(jcrPath);
                collectEffectiveCugs(oakPath, r, effective, visitedPaths);
            }
            return effective.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    //--------------------------------------------------------< PolicyOwner >---
    @Override
    public boolean defines(@Nullable String absPath, @NotNull AccessControlPolicy accessControlPolicy) {
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

    @Nullable
    private CugPolicy getCugPolicy(@NotNull String oakPath) throws RepositoryException {
        return getCugPolicy(oakPath, getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true), false);
    }

    @Nullable
    private CugPolicy getCugPolicy(@NotNull String oakPath, @NotNull Tree tree, boolean isEffective) {
        Tree cug = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cug)) {
            return new CugPolicyImpl(oakPath, getNamePathMapper(), principalManager, CugUtil.getImportBehavior(config), cugExclude, getPrincipals(cug), isEffective);
        } else {
            return null;
        }
    }

    private Iterable<Principal> getPrincipals(@NotNull Tree cugTree) {
        PropertyState property = cugTree.getProperty(REP_PRINCIPAL_NAMES);
        if (property == null) {
            return Collections.emptySet();
        } else {
            return StreamSupport.stream(property.getValue(Type.STRINGS).spliterator(), false).map(principalName -> {
                Principal principal = principalManager.getPrincipal(principalName);
                if (principal == null) {
                    log.debug("Unknown principal {}", principalName);
                    principal = new PrincipalImpl(principalName);
                }
                return principal;
            }).collect(Collectors.toList());
        }
    }

    private static boolean isValidPolicy(@Nullable String absPath, @NotNull AccessControlPolicy policy) {
        return policy instanceof CugPolicyImpl && ((CugPolicyImpl) policy).getPath().equals(absPath);
    }

    private static void checkValidPolicy(@Nullable String absPath, @NotNull AccessControlPolicy policy) throws AccessControlException {
        if (!(policy instanceof CugPolicyImpl)) {
            throw new AccessControlException("Unsupported policy implementation: " + policy);
        }

        CugPolicyImpl cug = (CugPolicyImpl) policy;
        if (!cug.getPath().equals(absPath)) {
            throw new AccessControlException("Path mismatch: Expected " + cug.getPath() + ", Found: " + absPath);
        }
    }
    
    private void collectEffectiveCugs(@Nullable String oakPath, @NotNull Root r, @NotNull List<AccessControlPolicy> effective, @NotNull Set<String> visitedPaths) {
        if (oakPath == null) {
            return;
        }
        PermissionProvider pp = getPermissionProvider();
        Tree t = r.getTree(oakPath);
        while (visitedPaths.add(oakPath)) {
            if (CugUtil.isSupportedPath(oakPath, supportedPaths) && t.exists() && pp.isGranted(t, null, Permissions.READ_ACCESS_CONTROL)) {
                CugPolicy cug = getCugPolicy(oakPath, r.getTree(oakPath), true);
                if (cug != null) {
                    effective.add(cug);
                }
            }
            if (PathUtils.denotesRoot(oakPath)) {
                return;
            }
            // walk up the hierarchy
            t = t.getParent();
            oakPath = PathUtils.getAncestorPath(oakPath, 1);
        }
    }

    private Set<String> collectEffectiveCandidates(@NotNull Root r, @NotNull Iterable<String> principalNames) {
        Root immutableRoot = rootProvider.createReadOnlyRoot(r);
        Set<String> candidates = new TreeSet<>();

        Queue<String> eval = new LinkedList<>();
        eval.add(PathUtils.ROOT_PATH);
        while (!eval.isEmpty()) {
            String path = eval.remove();
            Tree t = immutableRoot.getTree(path);
            if (PathUtils.denotesRoot(path)) {
                Iterables.addAll(eval, nestedCugPaths(t));
            }
            if (CugUtil.isSupportedPath(path, supportedPaths)) {
                Tree cug = CugUtil.getCug(t);
                PropertyState pNames = (cug == null) ? null : cug.getProperty(REP_PRINCIPAL_NAMES);
                if (pNames != null) {
                    if (!Collections.disjoint(CollectionUtils.toSet(principalNames), CollectionUtils.toSet(pNames.getValue(Type.STRINGS)))) {
                        candidates.add(path);
                    }
                    Iterables.addAll(eval, nestedCugPaths(cug));
                }
            }
        }
        return candidates;
    }

    private static Iterable<String> nestedCugPaths(@NotNull Tree t) {
        PropertyState nested = t.getProperty(HIDDEN_NESTED_CUGS);
        return (nested != null) ? nested.getValue(Type.STRINGS) : Collections.emptySet();
    }
}
