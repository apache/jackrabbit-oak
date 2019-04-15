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

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
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
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Implementation of the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}
 * interface that allows to create, modify and remove closed user group policies.
 */
class PrincipalBasedAccessControlManager extends AbstractAccessControlManager implements PolicyOwner, Constants {

    private static final Logger log = LoggerFactory.getLogger(PrincipalBasedAccessControlManager.class);

    private final MgrProvider mgrProvider;
    private final int importBehavior;

    private final PrincipalManager principalManager;
    private final PrivilegeBitsProvider privilegeBitsProvider;

    private final FilterProvider filterProvider;
    private final Filter filter;

    PrincipalBasedAccessControlManager(@NotNull MgrProvider mgrProvider,
                                       @NotNull FilterProvider filterProvider) {
        super(mgrProvider.getRoot(), mgrProvider.getNamePathMapper(), mgrProvider.getSecurityProvider());

        this.mgrProvider = mgrProvider;

        importBehavior = ImportBehavior.valueFromString(getConfig().getParameters().getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT));

        principalManager = mgrProvider.getPrincipalManager();
        privilegeBitsProvider = mgrProvider.getPrivilegeBitsProvider();

        this.filterProvider = filterProvider;
        filter = filterProvider.getFilter(mgrProvider.getSecurityProvider(), mgrProvider.getRoot(), mgrProvider.getNamePathMapper());
    }

    //-------------------------------------< JackrabbitAccessControlManager >---

    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) throws RepositoryException {
        if (canHandle(principal)) {
            String oakPath = filter.getOakPath(principal);
            Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);
            if (!tree.hasChild(REP_PRINCIPAL_POLICY)) {
                return new JackrabbitAccessControlPolicy[]{new PrincipalPolicyImpl(principal, oakPath, mgrProvider)};
            }
        }
        return new JackrabbitAccessControlPolicy[0];
    }

    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) throws RepositoryException {
        JackrabbitAccessControlPolicy policy = null;
        if (canHandle(principal)) {
            policy = createPolicy(principal, false);
        }
        return (policy == null) ? new JackrabbitAccessControlPolicy[0] : new JackrabbitAccessControlPolicy[]{policy};
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(Set<Principal> principals) throws RepositoryException {
        // this implementation only takes effect if the complete set of principals can be handled. see also
        // PrincipalBasedAuthorizationConfiguration.getPermissionProvider
        if (canHandle(principals)) {
            Set<AccessControlPolicy> effective = new HashSet<>(principals.size());
            for (Principal principal : principals) {
                AccessControlPolicy policy = createPolicy(principal, true);
                if (policy != null) {
                    effective.add(policy);
                }
            }
            return effective.toArray(new AccessControlPolicy[0]);
        } else {
            return new JackrabbitAccessControlPolicy[0];
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

            Map<String, List<JackrabbitAccessControlEntry>> m = new TreeMap<>();
            for (ResultRow row : result.getRows()) {
                Tree entryTree = row.getTree(null);
                String effectivePath = row.getValue(REP_EFFECTIVE_PATH).getValue(Type.STRING);
                List<JackrabbitAccessControlEntry> entries = m.computeIfAbsent(effectivePath, s -> new ArrayList<>());
                entries.add(createEffectiveEntry(entryTree, effectivePath));
            }
            Iterable<ImmutableACL> acls = Iterables.transform(m.entrySet(), entry -> new ImmutableACL(entry.getKey(), entry.getValue(), mgrProvider.getRestrictionProvider(), getNamePathMapper()));
            return Iterables.toArray(acls, ImmutableACL.class);
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
            String effectiveOakPath = Strings.nullToEmpty(entry.getOakPath());
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

    private boolean canHandle(@NotNull Collection<Principal> principals) throws AccessControlException {
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
                                                       boolean isEffectivePolicy) throws RepositoryException {
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
                    policy.addEntry(child);
                }
            }
        }
        if (isEffectivePolicy && policy != null) {
            return (policy.isEmpty()) ? null : new ImmutableACL(policy);
        } else {
            return policy;
        }
    }

    private boolean isAccessControlled(@NotNull Tree tree) {
        Tree typeRoot = getRoot().getTree(NodeTypeConstants.NODE_TYPES_PATH);
        return tree.exists() && TreeUtil.isNodeType(tree, MIX_REP_PRINCIPAL_BASED_MIXIN, typeRoot);
    }

    private Iterable<String> getEffectivePaths(@Nullable String oakPath) {
        // read-access-control permission has already been check for 'oakPath'
        List<String> paths = Lists.newArrayList();
        paths.add(Strings.nullToEmpty(oakPath));

        String effectivePath = oakPath;
        while (effectivePath != null && !PathUtils.denotesRoot(effectivePath)) {
            effectivePath = PathUtils.getParentPath(effectivePath);
            paths.add(effectivePath);
        }
        return paths;
    }

    @NotNull
    private JackrabbitAccessControlEntry createEffectiveEntry(@NotNull Tree entryTree, @NotNull String effectivePath) throws AccessControlException {
        String principalName = TreeUtil.getString(entryTree.getParent(), AccessControlConstants.REP_PRINCIPAL_NAME);
        PrivilegeBits bits = privilegeBitsProvider.getBits(entryTree.getProperty(Constants.REP_PRIVILEGES).getValue(Type.NAMES));
        Set<Restriction> restrictions = mgrProvider.getRestrictionProvider().readRestrictions(effectivePath, entryTree);
        return new EffectiveEntry(new PrincipalImpl(principalName), bits, true, restrictions, getNamePathMapper());
    }

    private final class EffectiveEntry extends ACE {
        private EffectiveEntry(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions, NamePathMapper namePathMapper) throws AccessControlException {
            super(principal, privilegeBits, isAllow, restrictions, namePathMapper);
        }

        @Override
        public Privilege[] getPrivileges() {
            Set<String> names =  privilegeBitsProvider.getPrivilegeNames(getPrivilegeBits());
            return Utils.privilegesFromOakNames(names, mgrProvider.getPrivilegeManager(), getNamePathMapper());
        }
    }
}
