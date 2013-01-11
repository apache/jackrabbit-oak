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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
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
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccessControlManagerImpl... TODO
 */
public class AccessControlManagerImpl implements JackrabbitAccessControlManager, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(AccessControlManagerImpl.class);

    private final Root root;
    private final NamePathMapper namePathMapper;

    private final PrivilegeManager privilegeManager;
    private final PrincipalProvider principalProvider;
    private final RestrictionProvider restrictionProvider;
    private final ReadOnlyNodeTypeManager ntMgr;

    public AccessControlManagerImpl(Root root, NamePathMapper namePathMapper,
                                    SecurityProvider securityProvider) {
        this.root = root;
        this.namePathMapper = namePathMapper;

        privilegeManager = securityProvider.getPrivilegeConfiguration().getPrivilegeManager(root, namePathMapper);
        principalProvider = securityProvider.getPrincipalConfiguration().getPrincipalProvider(root, namePathMapper);
        restrictionProvider = securityProvider.getAccessControlConfiguration().getRestrictionProvider(namePathMapper);
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
        // TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Privilege[] getPrivileges(String absPath) throws PathNotFoundException, RepositoryException {
        // TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        Tree tree = getTree(absPath);
        AccessControlPolicy policy = readACL(absPath, tree, false);
        if (policy != null) {
            return new AccessControlPolicy[] {policy};
        } else {
            return new AccessControlPolicy[0];
        }
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        Tree tree = getTree(absPath);
        List<AccessControlPolicy> effective = new ArrayList<AccessControlPolicy>();
        AccessControlPolicy policy = readACL(absPath, tree, true);
        if (policy != null) {
            effective.add(policy);
        }
        if (absPath != null) {
            String parentPath = Text.getRelativeParent(tree.getPath(), 1);
            while (!parentPath.isEmpty()) {
                Tree t = root.getTree(parentPath);
                AccessControlPolicy plc = readACL(parentPath, t, true);
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
        Tree tree = getTree(absPath);
        AccessControlPolicy policy = null;
        NodeUtil aclNode = getAclNode(absPath, tree);
        if (aclNode == null) {
            // create an empty acl unless the node is protected or cannot have
            // mixin set (e.g. due to a lock) or
            // has colliding rep:policy or rep:repoPolicy child node set.
            String aclName = getAclOakName(absPath);
            if (tree.hasChild(aclName)) {
                // policy child node without node being access controlled
                log.warn("Colliding policy child without node being access controllable ({}).", absPath);
            } else {
                String mixinName = getOakMixinName(absPath);
                if (ntMgr.isNodeType(tree, mixinName) || ntMgr.getEffectiveNodeType(tree).supportsMixin(mixinName)) {
                    policy = new NodeACL(absPath);
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
        checkValidPolicy(absPath, policy);

        if (policy instanceof PrincipalACL) {
            // TODO
            throw new RepositoryException("not yet implemented");
        } else {
            Tree tree = getTree(absPath);
            NodeUtil aclNode = getAclNode(absPath, tree);
            if (aclNode != null) {
                // remove all existing aces
                for (Tree aceTree : aclNode.getTree().getChildren()) {
                    aceTree.remove();
                }
            } else {
                aclNode = createAclTree(absPath, tree);
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
                        restrictions.add(restrictionProvider.createRestriction(acl.getPath(), rName, ace.getRestriction(rName)));
                    }
                }
                restrictionProvider.writeRestrictions(absPath, aceNode.getTree(), restrictions);
            }
        }
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        checkValidPolicy(absPath, policy);

        if (policy instanceof PrincipalACL) {
            // TODO
            throw new RepositoryException("not yet implemented");
        } else {
            Tree tree = getTree(absPath);
            NodeUtil aclNode = getAclNode(absPath, tree);
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
            return new JackrabbitAccessControlPolicy[] {createPrincipalACL(principal, null)};
        }
    }

    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) throws RepositoryException {
        Result aceResult = searchAces(Collections.<Principal>singleton(principal));
        if (aceResult.getSize() > 0) {
            return new JackrabbitAccessControlPolicy[] {createPrincipalACL(principal, aceResult)};
        } else {
            return new JackrabbitAccessControlPolicy[0];
        }
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(Set<Principal> principals) throws RepositoryException {
        Result aceResult = searchAces(principals);
        List<AccessControlPolicy> effective = new ArrayList<AccessControlPolicy>();
        for (ResultRow row : aceResult.getRows()) {
            Tree aclTree = root.getTree(row.getPath()).getParent();
            Tree accessControlledTree = aclTree.getParent();

            String path = (REP_REPO_POLICY.equals(aclTree.getName())) ? null : accessControlledTree.getPath();
            AccessControlPolicy policy = readACL(path, accessControlledTree, true);
            if (policy != null) {
                effective.add(policy);
            }
        }
        return effective.toArray(new AccessControlPolicy[effective.size()]);
    }

    @Override
    public boolean hasPrivileges(String absPath, Set<Principal> principals, Privilege[] privileges) throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Privilege[] getPrivileges(String absPath, Set<Principal> principals) throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Tree getTree(String jcrPath) throws RepositoryException {
        Tree tree;
        if (jcrPath == null) {
            tree = root.getTree("/");
        } else {
            String oakPath = namePathMapper.getOakPathKeepIndex(jcrPath);
            if (oakPath == null) {
                throw new RepositoryException("Failed to resolve JCR path " + jcrPath);
            }
            tree = root.getTree(oakPath);
        }
        if (tree == null) {
            throw new PathNotFoundException("No tree at " +jcrPath);
        }
        checkPermission(tree);
        checkIsAccessControlContent(tree);
        return tree;
    }

    private void checkPermission(Tree tree) throws AccessDeniedException {
        // TODO
    }

    private void checkValidPath(String jcrPath) throws RepositoryException {
        getTree(jcrPath);
    }

    /**
     * Check if the specified tree is itself defines access control related
     * content.
     *
     * @param tree the target tree
     * @throws AccessControlException If the tree represents a ACL or ACE item.
     */
    private static void checkIsAccessControlContent(Tree tree) throws AccessControlException {
        String ntName = new NodeUtil(tree).getPrimaryNodeTypeName();
        if (AC_NODE_TYPE_NAMES.contains(ntName)) {
            throw new AccessControlException("Tree " + tree.getPath() + " defines access control content.");
        }
    }

    private static void checkValidPolicy(String jcrPath, AccessControlPolicy policy) throws AccessControlException {
        if (policy instanceof ACL) {
            String path = ((ACL) policy).getPath();
            if ((path == null && jcrPath != null) || (path != null && !path.equals(jcrPath))) {
                throw new AccessControlException("Invalid access control policy " + policy + ": path mismatch " + jcrPath);
            }
        } else {
            throw new AccessControlException("Invalid access control policy " + policy);
        }
    }

    private boolean isAccessControlled(Tree tree, String nodeTypeName) throws RepositoryException {
        return tree != null && ntMgr.isNodeType(tree, nodeTypeName);
    }

    private boolean isACE(Tree tree) throws RepositoryException {
        return ntMgr.isNodeType(tree, namePathMapper.getJcrName(NT_REP_ACE));
    }

    @CheckForNull
    private AccessControlList readACL(String jcrPath, Tree accessControlledTree,
                        boolean isReadOnly) throws RepositoryException {
        AccessControlList acl = null;
        String aclName = getAclOakName(jcrPath);
        String mixinName = getOakMixinName(jcrPath);

        if (isAccessControlled(accessControlledTree, mixinName) && accessControlledTree.hasChild(aclName)) {
            Tree aclTree = accessControlledTree.getChild(aclName);
            List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>();
            for (Tree child : aclTree.getChildren()) {
                if (isACE(child)) {
                    entries.add(readACE(jcrPath, child, restrictionProvider));
                }
            }
            if (isReadOnly) {
                acl = new ImmutableACL(jcrPath, entries, restrictionProvider);
            } else {
                acl = new NodeACL(jcrPath, entries);
            }
        }
        return acl;
    }

    @Nonnull
    private JackrabbitAccessControlEntry readACE(String jcrPath, Tree aceTree, RestrictionProvider restrictionProvider)
            throws RepositoryException {
        NodeUtil aceNode = new NodeUtil(aceTree);
        Principal principal = principalProvider.getPrincipal(aceNode.getString(REP_PRINCIPAL_NAME, null));
        boolean isAllow = aceNode.hasPrimaryNodeTypeName(NT_REP_GRANT_ACE);
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(jcrPath, aceTree);
        return new ACE(principal, getPrivileges(aceNode), isAllow, restrictions);
    }

    private JackrabbitAccessControlList createPrincipalACL(Principal principal, Result aceResult) throws RepositoryException {
        // TODO: specific path indicating the principal-based nature of the
        // TODO: ACL... this could also be the path of the compiled permissions
        // TODO: for this principal.
        String principalBasedPath = null;
        // TODO: specific principal based restriction provider specifying a
        // TODO: mandatory 'path' restriction to enforce the location where
        // TODO: the ACEs need to be stored in the content tree.
        RestrictionProvider pbRestrictions = new PrincipalRestrictionProvider(namePathMapper);

        List<JackrabbitAccessControlEntry> entries = null;
        if (aceResult != null) {
            entries = new ArrayList();
            for (ResultRow row : aceResult.getRows()) {
                Tree aceTree = root.getTree(row.getPath());
                if (isACE(aceTree)) {
                    String aclPath = Text.getRelativeParent(aceTree.getPath(), 1);
                    String jcrPath;
                    if (aclPath.endsWith(REP_REPO_POLICY)) {
                        jcrPath = null;
                    } else {
                        jcrPath = Text.getRelativeParent(aclPath, 1);
                    }
                    entries.add(readACE(jcrPath, aceTree, pbRestrictions));
                }
            }
        }
        return new PrincipalACL(principalBasedPath, entries, pbRestrictions);
    }

    /**
     *
     * @param jcrPath the JCR path as specified with the ac mgr call.
     * @param tree the access controlled node.
     * @return the new acl tree.
     * @throws RepositoryException if an error occurs
     */
    @Nonnull
    private NodeUtil createAclTree(String jcrPath, Tree tree) throws RepositoryException {
        NodeUtil node = new NodeUtil(tree);
        String mixinName = getOakMixinName(jcrPath);

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
        return node.addChild(getAclOakName(jcrPath), namePathMapper.getJcrName(NT_REP_ACL));
    }

    @Nonnull
    private Result searchAces(Set<Principal> principals) throws RepositoryException {
        // TODO: review if compiled permissions could be used instead of running a query.
        // TODO: replace XPATH
        // TODO: specify sort order
        StringBuilder stmt = new StringBuilder("/jcr:root");
        stmt.append("//element(*,");
        stmt.append(NT_REP_ACE);
        stmt.append(")[");
        int i = 0;
        for (Principal principal : principals) {
            if (i > 0) {
                stmt.append(" or ");
            }
            stmt.append('@');
            stmt.append(ISO9075.encode(REP_PRINCIPAL_NAME));
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
    private Set<Privilege> getPrivileges(NodeUtil aceNode) throws RepositoryException {
        String[] privNames = aceNode.getNames(REP_PRIVILEGES);
        Set<Privilege> privileges = new HashSet<Privilege>(privNames.length);
        for (String name : privNames) {
            privileges.add(privilegeManager.getPrivilege(name));
        }
        return privileges;
    }

    @CheckForNull
    private static NodeUtil getAclNode(String jcrPath, Tree accessControlledTree) {
        Tree policyTree = accessControlledTree.getChild(getAclOakName(jcrPath));
        return (policyTree == null) ? null : new NodeUtil(policyTree);
    }

    @Nonnull
    private static String getOakMixinName(String jcrPath) {
        return (jcrPath == null) ? MIX_REP_REPO_ACCESS_CONTROLLABLE : MIX_REP_ACCESS_CONTROLLABLE;
    }

    @Nonnull
    private static String getAclOakName(String jcrPath) {
        return (jcrPath == null) ? REP_REPO_POLICY : REP_POLICY;
    }

    /**
     * Create a unique valid name for the Permission nodes to be save.
     *
     * @param aclNode a name for the child is resolved
     * @param isAllow If the ACE is allowing or denying.
     * @return the name of the ACE node.
     */
    @Nonnull
    public static String generateAceName(NodeUtil aclNode, boolean isAllow) {
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

        NodeACL(String jcrPath) {
            super(jcrPath);
        }

        NodeACL(String jcrPath, List<JackrabbitAccessControlEntry> entries) {
            super(jcrPath, entries);
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }
    }

    private class PrincipalACL extends ACL {

        private final RestrictionProvider restrictionProvider;
        private PrincipalACL(String jcrPath, List<JackrabbitAccessControlEntry> entries, RestrictionProvider restrictionProvider) {
            super(jcrPath, entries);
            this.restrictionProvider = restrictionProvider;
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }
    }

    private class PrincipalRestrictionProvider extends RestrictionProviderImpl {

        private PrincipalRestrictionProvider(NamePathMapper namePathMapper) {
            super(namePathMapper);
        }

        @Nonnull
        @Override
        public Set<RestrictionDefinition> getSupportedRestrictions(String jcrPath) {
            Set<RestrictionDefinition> definitions = new HashSet<RestrictionDefinition>(super.getSupportedRestrictions(jcrPath));
            definitions.add(new RestrictionDefinitionImpl(REP_NODE_PATH, PropertyType.PATH, true, namePathMapper));
            return definitions;
        }

        @Override
        public Set<Restriction> readRestrictions(String jcrPath, Tree aceTree) throws AccessControlException {
            Set<Restriction> restrictions = super.readRestrictions(jcrPath, aceTree);
            String value = (jcrPath == null) ? "" : jcrPath;
            PropertyState nodePathProp = PropertyStates.createProperty(REP_NODE_PATH, value, Type.PATH);
            restrictions.add(new RestrictionImpl(nodePathProp, true, namePathMapper));
            return restrictions;
        }

        @Override
        public void writeRestrictions(String jcrPath, Tree aceTree, Set<Restriction> restrictions) throws AccessControlException {
            Iterator<Restriction> it = restrictions.iterator();
            while (it.hasNext()) {
                Restriction r = it.next();
                if (REP_NODE_PATH.equals(r.getName())) {
                    it.remove();
                }
            }
            super.writeRestrictions(jcrPath, aceTree, restrictions);
        }
    }
}