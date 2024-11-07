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
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.QueryUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.util.ISO9075;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}
 * interface that allows to create, modify and remove closed user group policies.
 */
class PrincipalBasedAccessControlManager extends AbstractAccessControlManager implements PolicyOwner, Constants {

    private static final Logger log = LoggerFactory.getLogger(PrincipalBasedAccessControlManager.class);

    private final MgrProvider mgrProvider;
    private final int importBehavior;
    private final Set<String> readPaths;

    private final PrincipalManager principalManager;
    private final PrivilegeBitsProvider privilegeBitsProvider;

    private final FilterProvider filterProvider;
    private final Filter filter;

    PrincipalBasedAccessControlManager(@NotNull MgrProvider mgrProvider,
                                       @NotNull FilterProvider filterProvider) {
        super(mgrProvider.getRoot(), mgrProvider.getNamePathMapper(), mgrProvider.getSecurityProvider());

        this.mgrProvider = mgrProvider;

        ConfigurationParameters configParams = getConfig().getParameters();
        importBehavior = ImportBehavior.valueFromString(configParams.getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT));
        readPaths = configParams.getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);

        principalManager = mgrProvider.getPrincipalManager();
        privilegeBitsProvider = mgrProvider.getPrivilegeBitsProvider();

        this.filterProvider = filterProvider;
        filter = filterProvider.getFilter(mgrProvider.getSecurityProvider(), mgrProvider.getRoot(), mgrProvider.getNamePathMapper());
    }

    @Override
    protected @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
        return mgrProvider.getPrivilegeBitsProvider();
    }

    //-------------------------------------< JackrabbitAccessControlManager >---

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(@NotNull Principal principal) throws RepositoryException {
        if (canHandle(principal)) {
            String oakPath = filter.getOakPath(principal);
            Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);
            if (!tree.hasChild(REP_PRINCIPAL_POLICY)) {
                return new JackrabbitAccessControlPolicy[]{new PrincipalPolicyImpl(principal, oakPath, mgrProvider)};
            }
        }
        return new JackrabbitAccessControlPolicy[0];
    }

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(@NotNull Principal principal) throws RepositoryException {
        JackrabbitAccessControlPolicy policy = null;
        if (canHandle(principal)) {
            policy = createPolicy(principal, false, Collections.emptyList());
        }
        return (policy == null) ? new JackrabbitAccessControlPolicy[0] : new JackrabbitAccessControlPolicy[]{policy};
    }

    @NotNull
    @Override
    public AccessControlPolicy[] getEffectivePolicies(@NotNull Set<Principal> principals) throws RepositoryException {
        // this implementation only takes effect if the complete set of principals can be handled. see also
        // PrincipalBasedAuthorizationConfiguration.getPermissionProvider
        if (canHandle(principals)) {
            List<AccessControlPolicy> effective = new ArrayList<>(principals.size());
            for (Principal principal : principals) {
                AccessControlPolicy policy = createPolicy(principal, true, Collections.emptyList());
                if (policy != null) {
                    effective.add(policy);
                }
            }
            // add read-policy if there are configured paths
            if (ReadPolicy.canAccessReadPolicy(getPermissionProvider(), readPaths.toArray(new String[0]))) {
                effective.add(ReadPolicy.INSTANCE);
            }
            return effective.toArray(new AccessControlPolicy[0]);
        } else {
            return new JackrabbitAccessControlPolicy[0];
        }
    }

    @Override
    public @NotNull Iterator<AccessControlPolicy> getEffectivePolicies(@NotNull Set<Principal> principals, @Nullable String... absPaths) throws RepositoryException {
        // this implementation only takes effect if the complete set of principals can be handled. see also
        // PrincipalBasedAuthorizationConfiguration.getPermissionProvider
        if (canHandle(principals)) {
            Collection<String> oakPaths = getOakPaths(absPaths);
            List<AccessControlPolicy> effective = new ArrayList<>(principals.size());
            for (Principal principal : principals) {
                AccessControlPolicy policy = createPolicy(principal, true, oakPaths);
                if (policy != null) {
                    effective.add(policy);
                }
            }
            // add read-policy if there are configured paths
            boolean hasReadPolicy = oakPaths.isEmpty() || oakPaths.stream().anyMatch(p -> ReadPolicy.hasEffectiveReadPolicy(readPaths, p));
            if (hasReadPolicy) {
                effective.add(ReadPolicy.INSTANCE);
            }
            return effective.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    //-----------------------------------------------< AccessControlManager >---

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        getTree(getOakPath(absPath), Permissions.READ_ACCESS_CONTROL, true);

        log.debug("Editing access control policies by path is not supported. Use JackrabbitAccessControlManager.getPolicies(Principal principal)");
        return new AccessControlPolicy[0];
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);

        StringBuilder stmt = new StringBuilder(QueryConstants.SEARCH_ROOT_PATH);
        stmt.append(filterProvider.getFilterRoot());
        stmt.append("//element(*,").append(NT_REP_PRINCIPAL_ENTRY).append(")[");
        String cond = "";
        // list of effective paths not empty at this point and will at least contain the oakPath
        for (String effectivePath : getEffectivePaths(oakPath)) {
            stmt.append(cond);
            stmt.append("@").append(ISO9075.encode(REP_EFFECTIVE_PATH));
            stmt.append("='").append(QueryUtils.escapeForQuery(effectivePath));
            stmt.append("'");
            cond = " or ";
        }
        stmt.append("] order by jcr:path option (traversal ok)");

        try {
            // run query on persisted content omitting any transient modifications
            QueryEngine queryEngine = getLatestRoot().getQueryEngine();
            Result result = queryEngine.executeQuery(stmt.toString(), Query.XPATH, QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

            Map<Principal, List<AbstractEntry>> m = new HashMap<>();
            for (ResultRow row : result.getRows()) {
                Tree entryTree = row.getTree(null);
                AbstractEntry entry = createEffectiveEntry(entryTree);
                if (entry != null) {
                    List<AbstractEntry> entries = m.computeIfAbsent(entry.getPrincipal(), s -> new ArrayList<>());
                    entries.add(entry);
                }
            }
            Iterable<PrincipalAccessControlList> acls = Iterables.transform(m.entrySet(), entry -> new ImmutablePrincipalPolicy(entry.getKey(), filter.getOakPath(entry.getKey()), entry.getValue(), mgrProvider.getRestrictionProvider(), getNamePathMapper()));

            if (ReadPolicy.hasEffectiveReadPolicy(readPaths, oakPath)) {
                Iterable<AccessControlPolicy> iterable = Iterables.concat(acls, Collections.singleton(ReadPolicy.INSTANCE));
                return Iterables.toArray(iterable, AccessControlPolicy.class);
            } else {
                return Iterables.toArray(acls, PrincipalAccessControlList.class);
            }
        } catch (ParseException e) {
            String msg = "Error while collecting effective policies at " +absPath;
            log.error(msg, e);
            throw new RepositoryException(msg, e);
        }
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws RepositoryException {
        getTree(getOakPath(absPath), Permissions.READ_ACCESS_CONTROL, true);

        log.debug("Editing access control policies by path is not supported. Use JackrabbitAccessControlManager.getApplicablePolicies(Principal principal)");
        return AccessControlPolicyIteratorAdapter.EMPTY;
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        PrincipalPolicyImpl pp = checkValidPolicy(absPath, policy);
        String oakPath = pp.getOakPath();
        Tree tree = getTree(oakPath, Permissions.MODIFY_ACCESS_CONTROL, true);

        Tree policyTree = getPolicyTree(tree);
        if (policyTree.exists()) {
            policyTree.remove();
        }

        // make sure parent has mixin set and policy node is properly initialized
        TreeUtil.addMixin(tree, MIX_REP_PRINCIPAL_BASED_MIXIN, getRoot().getTree(NodeTypeConstants.NODE_TYPES_PATH), getRoot().getContentSession().getAuthInfo().getUserID());
        policyTree = TreeUtil.addChild(tree, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY);
        policyTree.setOrderableChildren(true);
        policyTree.setProperty(Constants.REP_PRINCIPAL_NAME, pp.getPrincipal().getName());

        int i = 0;
        RestrictionProvider restrictionProvider = mgrProvider.getRestrictionProvider();
        for (PrincipalPolicyImpl.EntryImpl entry : pp.getEntries()) {
            String effectiveOakPath = Objects.toString(entry.getOakPath(), "");
            Tree entryTree = TreeUtil.addChild(policyTree, "entry" + i++, NT_REP_PRINCIPAL_ENTRY);
            if (!Utils.hasModAcPermission(getPermissionProvider(), effectiveOakPath)) {
                throw new AccessDeniedException("Access denied.");
            }
            entryTree.setProperty(REP_EFFECTIVE_PATH, effectiveOakPath, Type.PATH);
            entryTree.setProperty(Constants.REP_PRIVILEGES, privilegeBitsProvider.getPrivilegeNames(entry.getPrivilegeBits()), Type.NAMES);
            restrictionProvider.writeRestrictions(oakPath, entryTree, entry.getRestrictions());
        }
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        PrincipalPolicyImpl pp = checkValidPolicy(absPath, policy);
        Tree policyTree = getPolicyTree(getTree(pp.getOakPath(), Permissions.MODIFY_ACCESS_CONTROL, true));
        if (policyTree.exists()) {
            for (Tree child : policyTree.getChildren()) {
                if (Utils.isPrincipalEntry(child)) {
                    PropertyState effectivePath = child.getProperty(REP_EFFECTIVE_PATH);
                    if (effectivePath == null) {
                        throw new AccessControlException("Missing mandatory property rep:effectivePath; cannot validate permissions to modify policy.");
                    } else if (!Utils.hasModAcPermission(getPermissionProvider(), effectivePath.getValue(Type.PATH))) {
                        throw new AccessDeniedException("Access denied.");
                    }
                }
            }
            policyTree.remove();
        } else {
            throw new AccessControlException("No policy to remove at " + absPath);
        }
    }

    //--------------------------------------------------------< PolicyOwner >---
    @Override
    public boolean defines(@Nullable String absPath, @NotNull AccessControlPolicy accessControlPolicy) {
        String oakPath = (absPath == null) ? null : getNamePathMapper().getOakPath(absPath);
        if (oakPath == null || !filterProvider.handlesPath(oakPath) ||  !(accessControlPolicy instanceof PrincipalPolicyImpl)) {
            return false;
        }
        return oakPath.equals(((PrincipalPolicyImpl) accessControlPolicy).getOakPath());
    }

    //--------------------------------------------------------------------------
    /**
     * Validate the specified {@code principal} taking the configured
     * {@link ImportBehavior} into account.
     *
     * @param principal The principal to validate.
     * @return if the principal can be handled by the filter
     * @throws AccessControlException If the principal has an invalid name or
     * if {@link ImportBehavior#ABORT} is configured and this principal cannot be handled by the filter.
     */
    private boolean canHandle(@Nullable Principal principal) throws AccessControlException {
        String name = (principal == null) ? null : principal.getName();
        if (Strings.isNullOrEmpty(name)) {
            throw new AccessControlException("Invalid principal " + name);
        }
        if (importBehavior ==  ImportBehavior.ABORT || importBehavior == ImportBehavior.IGNORE) {
            principal = principalManager.getPrincipal(name);
            if (principal == null) {
                if (importBehavior == ImportBehavior.IGNORE) {
                    log.debug("Ignoring unknown principal {}", name);
                    return false;
                } else {
                    // abort
                    throw new AccessControlException("Unsupported principal " + name);
                }
            }
        }
        return filter.canHandle(Collections.singleton(principal));
    }

    boolean canHandle(@NotNull Set<Principal> principals) throws AccessControlException {
        for (Principal principal : principals) {
            if (!canHandle(principal)) {
                return false;
            }
        }
        return true;
    }

    private PrincipalPolicyImpl checkValidPolicy(@Nullable String absPath, @NotNull AccessControlPolicy policy) throws AccessControlException {
        if (!defines(absPath, policy)) {
            throw new AccessControlException("Invalid policy "+ policy + " at path " + absPath);
        }
        return (PrincipalPolicyImpl) policy;
    }

    @NotNull
    private static Tree getPolicyTree(@NotNull Tree accessControlledTree) {
        return accessControlledTree.getChild(Constants.REP_PRINCIPAL_POLICY);
    }

    @Nullable
    private JackrabbitAccessControlPolicy createPolicy(@NotNull Principal principal,
                                                       boolean isEffectivePolicy,
                                                       @NotNull Collection<String> oakPaths) throws RepositoryException {
        String oakPath = filter.getOakPath(principal);
        Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);
        if (isEffectivePolicy) {
            Root r = getRoot().getContentSession().getLatestRoot();
            tree = r.getTree(tree.getPath());
        }

        if (!isAccessControlled(tree)) {
            return null;
        }

        PrincipalPolicyImpl policy = null;
        Tree policyTree = getPolicyTree(tree);
        if (Utils.isPrincipalPolicyTree(policyTree)) {
            policy = new PrincipalPolicyImpl(principal, oakPath, mgrProvider);
            for (Tree child : policyTree.getChildren()) {
                if (Utils.isPrincipalEntry(child)) {
                    policy.addEntry(child, oakPaths);
                }
            }
        }
        if (isEffectivePolicy && policy != null) {
            return (policy.isEmpty()) ? null : new ImmutablePrincipalPolicy(policy);
        } else {
            return policy;
        }
    }

    private boolean isAccessControlled(@NotNull Tree tree) {
        Tree typeRoot = getRoot().getTree(NodeTypeConstants.NODE_TYPES_PATH);
        return tree.exists() && TreeUtil.isNodeType(tree, MIX_REP_PRINCIPAL_BASED_MIXIN, typeRoot);
    }

    private static Iterable<String> getEffectivePaths(@Nullable String oakPath) {
        // read-access-control permission has already been check for 'oakPath'
        List<String> paths = new ArrayList<>();
        paths.add(Objects.toString(oakPath, ""));

        String effectivePath = oakPath;
        while (effectivePath != null && !PathUtils.denotesRoot(effectivePath)) {
            effectivePath = PathUtils.getParentPath(effectivePath);
            paths.add(effectivePath);
        }
        return paths;
    }

    @Nullable
    private AbstractEntry createEffectiveEntry(@NotNull Tree entryTree) throws AccessControlException {
        String principalName = TreeUtil.getString(entryTree.getParent(), AccessControlConstants.REP_PRINCIPAL_NAME);
        Principal principal = principalManager.getPrincipal(principalName);
        if (principal == null || !filter.canHandle(Collections.singleton(principal))) {
            return null;
        }
        String oakPath = Strings.emptyToNull(TreeUtil.getString(entryTree, REP_EFFECTIVE_PATH));
        PrivilegeBits bits = privilegeBitsProvider.getBits(entryTree.getProperty(Constants.REP_PRIVILEGES).getValue(Type.NAMES));

        RestrictionProvider rp = mgrProvider.getRestrictionProvider();
        if (!Utils.hasValidRestrictions(oakPath, entryTree, rp)) {
            return null;
        }

        Set<Restriction> restrictions = Utils.readRestrictions(rp, oakPath, entryTree);
        NamePathMapper npMapper = getNamePathMapper();
        return new AbstractEntry(oakPath, principal, bits, restrictions, npMapper) {
            @Override
            @NotNull NamePathMapper getNamePathMapper() {
                return npMapper;
            }

            @Override
            protected @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
                return privilegeBitsProvider;
            }

            @Override
            public Privilege[] getPrivileges() {
                Set<String> names =  privilegeBitsProvider.getPrivilegeNames(getPrivilegeBits());
                return Utils.privilegesFromOakNames(names, mgrProvider.getPrivilegeManager(), getNamePathMapper());
            }
        };
    }
}
