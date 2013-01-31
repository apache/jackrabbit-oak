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

import java.security.AccessController;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.authorization.restriction.PrincipalRestrictionProvider;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AccessControlManagerImpl... TODO
 */
public class AccessControlManagerImpl implements JackrabbitAccessControlManager, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(AccessControlManagerImpl.class);

    private final Root root;
    private final NamePathMapper namePathMapper;
    private final AccessControlConfiguration acConfig;

    private final PrivilegeManager privilegeManager;
    private final PrincipalManager principalManager;
    private final PermissionProvider permissionProvider;
    private final RestrictionProvider restrictionProvider;
    private final ReadOnlyNodeTypeManager ntMgr;

    public AccessControlManagerImpl(Root root, NamePathMapper namePathMapper,
                                    SecurityProvider securityProvider) {
        this.root = root;
        this.namePathMapper = namePathMapper;

        privilegeManager = securityProvider.getPrivilegeConfiguration().getPrivilegeManager(root, namePathMapper);
        principalManager = securityProvider.getPrincipalConfiguration().getPrincipalManager(root, namePathMapper);

        acConfig = securityProvider.getAccessControlConfiguration();

        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<Principal> principals = (subject != null) ? subject.getPrincipals() : Collections.<Principal>emptySet();
        permissionProvider = acConfig.getPermissionProvider(root, principals);
        restrictionProvider = acConfig.getRestrictionProvider(namePathMapper);
        ntMgr = ReadOnlyNodeTypeManager.getInstance(root, namePathMapper);
    }

    //-----------------------------------------------< AccessControlManager >---
    @Override
    public Privilege[] getSupportedPrivileges(String absPath) throws RepositoryException {
        checkValidPath(absPath);
        return privilegeManager.getRegisteredPrivileges();
    }

    @Override
    public Privilege privilegeFromName(String privilegeName) throws RepositoryException {
        return privilegeManager.getPrivilege(privilegeName);
    }

    @Override
    public boolean hasPrivileges(String absPath, Privilege[] privileges) throws RepositoryException {
        return hasPrivileges(absPath, privileges, permissionProvider);
    }

    @Override
    public Privilege[] getPrivileges(String absPath) throws RepositoryException {
        return getPrivileges(absPath, permissionProvider);
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath);
        AccessControlPolicy policy = createACL(oakPath, tree, false);
        if (policy != null) {
            return new AccessControlPolicy[]{policy};
        } else {
            return new AccessControlPolicy[0];
        }
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath);
        List<AccessControlPolicy> effective = new ArrayList<AccessControlPolicy>();
        AccessControlPolicy policy = createACL(oakPath, tree, true);
        if (policy != null) {
            effective.add(policy);
        }
        if (oakPath != null) {
            String parentPath = Text.getRelativeParent(oakPath, 1);
            while (!parentPath.isEmpty()) {
                Tree t = root.getTree(parentPath);
                AccessControlPolicy plc = createACL(parentPath, t, true);
                if (plc != null) {
                    effective.add(plc);
                }
                parentPath = (PathUtils.denotesRoot(parentPath)) ? "" : Text.getRelativeParent(parentPath, 1);
            }
        }
        return effective.toArray(new AccessControlPolicy[effective.size()]);
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath);

        AccessControlPolicy policy = null;
        NodeUtil aclNode = getAclNode(oakPath, tree);
        if (aclNode == null) {
            if (tree.hasChild(getAclName(oakPath))) {
                // policy child node without tree being access controlled
                log.warn("Colliding policy child without node being access controllable ({}).", absPath);
            } else {
                // create an empty acl unless the node is protected or cannot have
                // mixin set (e.g. due to a lock)
                String mixinName = getMixinName(oakPath);
                if (ntMgr.isNodeType(tree, mixinName) || ntMgr.getEffectiveNodeType(tree).supportsMixin(mixinName)) {
                    policy = new NodeACL(oakPath);
                } else {
                    log.warn("Node {} cannot be made access controllable.", absPath);
                }
            }
        } // else: acl already present -> getPolicies must be used.

        if (policy == null) {
            return AccessControlPolicyIteratorAdapter.EMPTY;
        } else {
            return new AccessControlPolicyIteratorAdapter(Collections.singleton(policy));
        }
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        checkValidPolicy(oakPath, policy);

        if (policy instanceof PrincipalACL) {
            // TODO
            throw new RepositoryException("not yet implemented");
        } else {
            Tree tree = getTree(oakPath);
            NodeUtil aclNode = getAclNode(oakPath, tree);
            if (aclNode != null) {
                // remove all existing aces
                for (Tree aceTree : aclNode.getTree().getChildren()) {
                    aceTree.remove();
                }
            } else {
                aclNode = createAclTree(oakPath, tree);
            }

            ACL acl = (ACL) policy;
            for (JackrabbitAccessControlEntry ace : acl.getEntries()) {
                boolean isAllow = ace.isAllow();
                String nodeName = generateAceName(aclNode, isAllow);
                String ntName = (isAllow) ? NT_REP_GRANT_ACE : NT_REP_DENY_ACE;

                NodeUtil aceNode = aclNode.addChild(nodeName, ntName);
                aceNode.setString(REP_PRINCIPAL_NAME, ace.getPrincipal().getName());
                aceNode.setNames(REP_PRIVILEGES, AccessControlUtils.namesFromPrivileges(ace.getPrivileges()));
                Set<Restriction> restrictions;
                if (ace instanceof ACE) {
                    restrictions = ((ACE) ace).getRestrictions();
                } else {
                    String[] rNames = ace.getRestrictionNames();
                    restrictions = new HashSet<Restriction>(rNames.length);
                    for (String rName : rNames) {
                        restrictions.add(restrictionProvider.createRestriction(oakPath, rName, ace.getRestriction(rName)));
                    }
                }
                restrictionProvider.writeRestrictions(oakPath, aceNode.getTree(), restrictions);
            }
        }
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        checkValidPolicy(oakPath, policy);

        if (policy instanceof PrincipalACL) {
            // TODO
            throw new RepositoryException("not yet implemented");
        } else {
            Tree tree = getTree(oakPath);
            NodeUtil aclNode = getAclNode(oakPath, tree);
            if (aclNode != null) {
                aclNode.getTree().remove();
            } else {
                throw new AccessControlException("No policy to remove at " + absPath);
            }
        }
    }

    //-------------------------------------< JackrabbitAccessControlManager >---
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) throws RepositoryException {
        Result aceResult = searchAces(Collections.<Principal>singleton(principal));
        if (aceResult.getSize() > 0) {
            return new JackrabbitAccessControlPolicy[0];
        } else {
            return new JackrabbitAccessControlPolicy[]{createPrincipalACL(principal, null)};
        }
    }

    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) throws RepositoryException {
        Result aceResult = searchAces(Collections.<Principal>singleton(principal));
        if (aceResult.getSize() > 0) {
            return new JackrabbitAccessControlPolicy[]{createPrincipalACL(principal, aceResult)};
        } else {
            return new JackrabbitAccessControlPolicy[0];
        }
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(Set<Principal> principals) throws RepositoryException {
        Result aceResult = searchAces(principals);
        List<AccessControlPolicy> effective = new ArrayList<AccessControlPolicy>();
        for (ResultRow row : aceResult.getRows()) {
            String acePath = row.getPath();
            String aclName = Text.getName(Text.getRelativeParent(acePath, 1));
            Tree accessControlledTree = root.getTree(Text.getRelativeParent(acePath, 2));

            if (aclName.isEmpty() || accessControlledTree == null) {
                log.debug("Isolated access control entry -> ignore query result at " + acePath);
                continue;
            }

            String path = (REP_REPO_POLICY.equals(aclName)) ? null : accessControlledTree.getPath();
            AccessControlPolicy policy = createACL(path, accessControlledTree, true);
            if (policy != null) {
                effective.add(policy);
            }
        }
        return effective.toArray(new AccessControlPolicy[effective.size()]);
    }

    @Override
    public boolean hasPrivileges(String absPath, Set<Principal> principals, Privilege[] privileges) throws RepositoryException {
        PermissionProvider provider = acConfig.getPermissionProvider(root, principals);
        return hasPrivileges(absPath, privileges, provider);
    }

    @Override
    public Privilege[] getPrivileges(String absPath, Set<Principal> principals) throws RepositoryException {
        PermissionProvider provider = acConfig.getPermissionProvider(root, principals);
        return getPrivileges(absPath, provider);
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private String getOakPath(String jcrPath) throws RepositoryException {
        if (jcrPath == null) {
            return null;
        } else {
            String oakPath = namePathMapper.getOakPathKeepIndex(jcrPath);
            if (oakPath == null || !PathUtils.isAbsolute(oakPath)) {
                throw new RepositoryException("Failed to resolve JCR path " + jcrPath);
            }
            return oakPath;
        }
    }

    @Nonnull
    private Tree getTree(@Nullable String oakPath) throws RepositoryException {
        Tree tree = (oakPath == null) ? root.getTree("/") : root.getTree(oakPath);
        if (tree == null) {
            throw new PathNotFoundException("No tree at " + oakPath);
        }
        checkPermission(tree);

        // check if the tree is access controlled
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        if (AC_NODETYPE_NAMES.contains(ntName)) {
            throw new AccessControlException("Tree " + tree.getPath() + " defines access control content.");
        }
        return tree;
    }

    private void checkPermission(@Nonnull Tree tree) throws AccessDeniedException {
        // TODO
    }

    private void checkValidPath(@Nullable String jcrPath) throws RepositoryException {
        getTree(getOakPath(jcrPath));
    }

    private static void checkValidPolicy(@Nullable String oakPath, @Nullable AccessControlPolicy policy) throws AccessControlException {
        if (policy instanceof ACL) {
            String path = ((ACL) policy).getOakPath();
            if ((path == null && oakPath != null) || (path != null && !path.equals(oakPath))) {
                throw new AccessControlException("Invalid access control policy " + policy + ": path mismatch " + oakPath);
            }
        } else {
            throw new AccessControlException("Invalid access control policy " + policy);
        }
    }

    private boolean isAccessControlled(@Nonnull Tree tree, @Nonnull String nodeTypeName) throws RepositoryException {
        return ntMgr.isNodeType(tree, nodeTypeName);
    }

    private boolean isACE(@Nonnull Tree tree) throws RepositoryException {
        return ntMgr.isNodeType(tree, NT_REP_ACE);
    }

    /**
     * @param oakPath the Oak path as specified with the ac mgr call.
     * @param tree    the access controlled node.
     * @return the new acl tree.
     * @throws RepositoryException if an error occurs
     */
    @Nonnull
    private NodeUtil createAclTree(@Nullable String oakPath, @Nonnull Tree tree) throws RepositoryException {
        String mixinName = getMixinName(oakPath);

        if (!isAccessControlled(tree, mixinName)) {
            PropertyState mixins = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
            if (mixins == null) {
                tree.setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singleton(mixinName), Type.NAMES);
            } else {
                PropertyBuilder pb = MemoryPropertyBuilder.copy(Type.NAME, mixins);
                pb.addValue(mixinName);
                tree.setProperty(pb.getPropertyState());
            }
        }
        return new NodeUtil(tree).addChild(getAclName(oakPath), NT_REP_ACL);
    }

    @CheckForNull
    private AccessControlList createACL(@Nullable String oakPath,
                                        @Nullable Tree accessControlledTree,
                                        boolean isReadOnly) throws RepositoryException {
        AccessControlList acl = null;
        String aclName = getAclName(oakPath);
        String mixinName = getMixinName(oakPath);

        if (accessControlledTree != null && isAccessControlled(accessControlledTree, mixinName)) {
            Tree aclTree = accessControlledTree.getChild(aclName);
            if (aclTree != null) {
                List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>();
                for (Tree child : aclTree.getChildren()) {
                    if (isACE(child)) {
                        entries.add(createACE(oakPath, child, restrictionProvider));
                    }
                }
                if (isReadOnly) {
                    acl = new ImmutableACL(oakPath, entries, restrictionProvider, namePathMapper);
                } else {
                    acl = new NodeACL(oakPath, entries);
                }
            }
        }
        return acl;
    }

    @Nonnull
    private JackrabbitAccessControlList createPrincipalACL(@Nonnull Principal principal,
                                                           @Nullable Result aceResult) throws RepositoryException {
        if (!(principal instanceof ItemBasedPrincipal)) {
            throw new IllegalArgumentException("Item based principal expected");
        }
        String principalPath = ((ItemBasedPrincipal) principal).getPath();
        RestrictionProvider restrProvider = new PrincipalRestrictionProvider(restrictionProvider, namePathMapper);

        List<JackrabbitAccessControlEntry> entries = null;
        if (aceResult != null) {
            entries = new ArrayList();
            for (ResultRow row : aceResult.getRows()) {
                Tree aceTree = root.getTree(row.getPath());
                if (isACE(aceTree)) {
                    String aclPath = Text.getRelativeParent(aceTree.getPath(), 1);
                    String path;
                    if (aclPath.endsWith(REP_REPO_POLICY)) {
                        path = null;
                    } else {
                        path = Text.getRelativeParent(aclPath, 1);
                    }
                    entries.add(createACE(path, aceTree, restrProvider));
                }
            }
        }
        return new PrincipalACL(principalPath, entries, restrProvider);
    }

    @Nonnull
    private JackrabbitAccessControlEntry createACE(String oakPath, Tree aceTree,
                                                   RestrictionProvider restrictionProvider) throws RepositoryException {
        boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(oakPath, aceTree);
        return new ACE(getPrincipal(aceTree), getPrivileges(aceTree), isAllow, restrictions);
    }

    @Nonnull
    private Result searchAces(@Nonnull Set<Principal> principals) throws RepositoryException {
        // TODO: replace XPATH
        // TODO: specify sort order
        StringBuilder stmt = new StringBuilder("/jcr:root");
        stmt.append("//element(*,");
        stmt.append(namePathMapper.getJcrName(NT_REP_ACE));
        stmt.append(")[");
        int i = 0;
        for (Principal principal : principals) {
            if (i > 0) {
                stmt.append(" or ");
            }
            stmt.append('@');
            stmt.append(ISO9075.encode(namePathMapper.getJcrName(REP_PRINCIPAL_NAME)));
            stmt.append("='");
            stmt.append(principal.getName().replaceAll("'", "''"));
            stmt.append('\'');
            i++;
        }
        stmt.append(']');

        try {
            QueryEngine queryEngine = root.getQueryEngine();
            return queryEngine.executeQuery(stmt.toString(), Query.XPATH, Long.MAX_VALUE, 0, Collections.<String, PropertyValue>emptyMap(), NamePathMapper.DEFAULT);
        } catch (ParseException e) {
            String msg = "Error while collecting effective policies.";
            log.error(msg, e.getMessage());
            throw new RepositoryException(msg, e);
        }
    }

    @Nonnull
    private Principal getPrincipal(@Nonnull Tree aceTree) {
        String principalName = checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
        Principal principal = principalManager.getPrincipal(principalName);
        if (principal == null) {
            log.debug("Unknown principal " + principalName);
            principal = new PrincipalImpl(principalName);
        }
        return principal;
    }

    @Nonnull
    private Set<Privilege> getPrivileges(@Nonnull Tree aceTree) throws RepositoryException {
        String[] privNames = TreeUtil.getStrings(aceTree, REP_PRIVILEGES);
        Set<Privilege> privileges = new HashSet<Privilege>(privNames.length);
        for (String name : privNames) {
            privileges.add(privilegeManager.getPrivilege(name));
        }
        return privileges;
    }

    @Nonnull
    private Privilege[] getPrivileges(String absPath, PermissionProvider provider) throws RepositoryException {
        // TODO
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath);
        Set<String> pNames = provider.getPrivilegeNames(tree);
        if (pNames.isEmpty()) {
            return new Privilege[0];
        } else {
            Set<Privilege> privileges = new HashSet<Privilege>(pNames.size());
            for (String name : pNames) {
                privileges.add(privilegeManager.getPrivilege(namePathMapper.getJcrName(name)));
            }
            return privileges.toArray(new Privilege[privileges.size()]);
        }
    }

    private boolean hasPrivileges(String absPath, Privilege[] privileges, PermissionProvider provider) throws RepositoryException {
        // TODO
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath);
        Set<String> privilegeNames = new HashSet<String>(privileges.length);
        for (Privilege privilege : privileges) {
            privilegeNames.add(namePathMapper.getOakName(privilege.getName()));
        }
        return provider.hasPrivileges(tree, privilegeNames.toArray(new String[privilegeNames.size()]));
    }

    @CheckForNull
    private NodeUtil getAclNode(@Nullable String oakPath, @Nonnull Tree accessControlledTree) throws RepositoryException {
        if (isAccessControlled(accessControlledTree, getMixinName(oakPath))) {
            Tree policyTree = accessControlledTree.getChild(getAclName(oakPath));
            if (policyTree != null) {
                return new NodeUtil(policyTree);
            }
        }

        return null;
    }

    @Nonnull
    private static String getMixinName(@Nullable String oakPath) {
        return (oakPath == null) ? MIX_REP_REPO_ACCESS_CONTROLLABLE : MIX_REP_ACCESS_CONTROLLABLE;
    }

    @Nonnull
    private static String getAclName(@Nullable String oakPath) {
        return (oakPath == null) ? REP_REPO_POLICY : REP_POLICY;
    }

    /**
     * Create a unique valid name for the Permission nodes to be save.
     *
     * @param aclNode a name for the child is resolved
     * @param isAllow If the ACE is allowing or denying.
     * @return the name of the ACE node.
     */
    @Nonnull
    private static String generateAceName(@Nonnull NodeUtil aclNode, boolean isAllow) {
        int i = 0;
        String hint = (isAllow) ? "allow" : "deny";
        String aceName = hint;
        while (aclNode.hasChild(aceName)) {
            aceName = hint + i;
            i++;
        }
        return aceName;
    }

    //--------------------------------------------------------------------------
    // TODO review again
    private class NodeACL extends ACL {

        NodeACL(String oakPath) {
            super(oakPath, namePathMapper);
        }

        NodeACL(String oakPath, List<JackrabbitAccessControlEntry> entries) {
            super(oakPath, entries, namePathMapper);
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }

        @Override
        PrincipalManager getPrincipalManager() {
            return principalManager;
        }

        @Override
        PrivilegeManager getPrivilegeManager() {
            return privilegeManager;
        }
    }

    private final class PrincipalACL extends NodeACL {

        private final RestrictionProvider rProvider;

        private PrincipalACL(String oakPath, List<JackrabbitAccessControlEntry> entries,
                             RestrictionProvider restrictionProvider) {
            super(oakPath, entries);
            rProvider = restrictionProvider;
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return rProvider;
        }
    }
}