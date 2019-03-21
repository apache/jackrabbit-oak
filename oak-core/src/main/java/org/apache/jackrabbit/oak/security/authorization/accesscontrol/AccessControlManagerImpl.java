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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.query.Query;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.NamedAccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
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
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionUtil;
import org.apache.jackrabbit.oak.security.authorization.restriction.PrincipalRestrictionProvider;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.util.ISO9075;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of the {@code JackrabbitAccessControlManager} interface.
 * This implementation covers both editing access control content by path and
 * by {@code Principal} resulting both in the same content structure.
 */
public class AccessControlManagerImpl extends AbstractAccessControlManager implements PolicyOwner {

    private static final Logger log = LoggerFactory.getLogger(AccessControlManagerImpl.class);

    private final PrivilegeBitsProvider bitsProvider;
    private final ReadOnlyNodeTypeManager ntMgr;

    private final PrincipalManager principalManager;
    private final RestrictionProvider restrictionProvider;

    private final ConfigurationParameters configParams;
    private final Set<String> readPaths;

    public AccessControlManagerImpl(@NotNull Root root, @NotNull NamePathMapper namePathMapper,
                                    @NotNull SecurityProvider securityProvider) {
        super(root, namePathMapper, securityProvider);

        bitsProvider = new PrivilegeBitsProvider(root);
        ntMgr = ReadOnlyNodeTypeManager.getInstance(root, namePathMapper);

        principalManager = securityProvider.getConfiguration(PrincipalConfiguration.class).getPrincipalManager(root, namePathMapper);
        restrictionProvider = getConfig().getRestrictionProvider();

        configParams = getConfig().getParameters();
        readPaths = configParams.getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
    }

    //-----------------------------------------------< AccessControlManager >---
    @NotNull
    @Override
    public AccessControlPolicy[] getPolicies(@Nullable String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);
        AccessControlPolicy policy = createACL(oakPath, tree, false, Predicates.alwaysTrue());

        List<AccessControlPolicy> policies = new ArrayList<>(2);
        if (policy != null) {
            policies.add(policy);
        }
        if (readPaths.contains(oakPath)) {
            policies.add(ReadPolicy.INSTANCE);
        }
        return policies.toArray(new AccessControlPolicy[0]);
    }

    @NotNull
    @Override
    public AccessControlPolicy[] getEffectivePolicies(@Nullable String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);

        Root r = getRoot().getContentSession().getLatestRoot();
        tree = r.getTree(tree.getPath());

        List<AccessControlPolicy> effective = new ArrayList<>();
        AccessControlPolicy policy = createACL(oakPath, tree, true, Predicates.alwaysTrue());
        if (policy != null) {
            effective.add(policy);
        }
        if (oakPath != null) {
            String parentPath = Text.getRelativeParent(oakPath, 1);
            while (!parentPath.isEmpty()) {
                Tree t = r.getTree(parentPath);
                AccessControlPolicy plc = createACL(parentPath, t, true, Predicates.alwaysTrue());
                if (plc != null) {
                    effective.add(plc);
                }
                parentPath = (PathUtils.denotesRoot(parentPath)) ? "" : Text.getRelativeParent(parentPath, 1);
            }
        }
        if (readPaths.contains(oakPath)) {
            effective.add(ReadPolicy.INSTANCE);
        }
        return effective.toArray(new AccessControlPolicy[0]);
    }

    @NotNull
    @Override
    public AccessControlPolicyIterator getApplicablePolicies(@Nullable String absPath) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Tree tree = getTree(oakPath, Permissions.READ_ACCESS_CONTROL, true);

        AccessControlPolicy policy = null;
        Tree aclTree = getAclTree(oakPath, tree);
        if (aclTree == null) {
            if (tree.hasChild(Util.getAclName(oakPath))) {
                // policy child node without tree being access controlled
                log.warn("Colliding policy child without node being access controllable ({}).", absPath);
            } else {
                // create an empty acl unless the node is protected or cannot have
                // mixin set (e.g. due to a lock)
                String mixinName = Util.getMixinName(oakPath);
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
    public void setPolicy(@Nullable String absPath, @NotNull AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Util.checkValidPolicy(oakPath, policy);

        if (policy instanceof PrincipalACL) {
            setPrincipalBasedAcl((PrincipalACL) policy);
        } else {
            Tree tree = getTree(oakPath, Permissions.MODIFY_ACCESS_CONTROL, true);
            setNodeBasedAcl(oakPath, tree, (ACL) policy);
        }
    }

    private void setPrincipalBasedAcl(PrincipalACL principalAcl) throws RepositoryException {
        AccessControlPolicy[] plcs = getPolicies(principalAcl.principal);
        PrincipalACL existing = (plcs.length == 0) ? null : (PrincipalACL) plcs[0];

        List<ACE> toAdd = Lists.newArrayList(principalAcl.getEntries());
        List<ACE> toRemove = Collections.emptyList();
        if (existing != null) {
            toAdd.removeAll(existing.getEntries());
            toRemove = existing.getEntries();
            toRemove.removeAll(principalAcl.getEntries());
        }

        // add new entries
        for (ACE ace : toAdd) {
            String path = getNodePath(ace);
            Tree tree = getTree(path, Permissions.MODIFY_ACCESS_CONTROL, true);

            ACL acl = (ACL) createACL(path, tree, false, Predicates.alwaysTrue());
            if (acl == null) {
                acl = new NodeACL(path);
            }

            // calculate single and mv restriction and drop the rep:nodePath restriction
            // present with the principal-based-entries.
            Map<String, Value> restrictions = new HashMap<>();
            Map<String, Value[]> mvRestrictions = new HashMap<>();
            for (Restriction r : ace.getRestrictions()) {
                String name = r.getDefinition().getName();
                if (REP_NODE_PATH.equals(name)) {
                    continue;
                }
                if (r.getDefinition().getRequiredType().isArray()) {
                    mvRestrictions.put(name, ace.getRestrictions(name));
                } else {
                    restrictions.put(name, ace.getRestriction(name));
                }
            }
            acl.addEntry(ace.getPrincipal(), ace.getPrivileges(), ace.isAllow(), restrictions, mvRestrictions);
            setNodeBasedAcl(path, tree, acl);
        }

        // remove entries that are not longer present in the acl to write
        for (ACE ace : toRemove) {
            String path = getNodePath(ace);
            Tree tree = getTree(path, Permissions.MODIFY_ACCESS_CONTROL, true);

            ACL acl = (ACL) createACL(path, tree, false, Predicates.alwaysTrue());
            if (acl != null) {
                // remove rep:nodePath restriction before removing the entry from
                // the node-based policy (see above for adding entries without
                // this special restriction).
                Set<Restriction> rstr = Sets.newHashSet(ace.getRestrictions());
                rstr.removeIf(r -> REP_NODE_PATH.equals(r.getDefinition().getName()));
                acl.removeAccessControlEntry(new Entry(ace.getPrincipal(), ace.getPrivilegeBits(), ace.isAllow(), rstr, getNamePathMapper()));
                setNodeBasedAcl(path, tree, acl);
            } else {
                log.debug("Missing ACL at {}; cannot remove entry {}", path, ace);
            }
        }
    }

    private void setNodeBasedAcl(@Nullable String oakPath, @NotNull Tree tree,
                                 @NotNull ACL acl) throws RepositoryException {
        Tree aclTree = getAclTree(oakPath, tree);
        if (aclTree != null) {
            // remove all existing aces
            for (Tree aceTree : aclTree.getChildren()) {
                aceTree.remove();
            }
        } else {
            aclTree = createAclTree(oakPath, tree);
        }
        aclTree.setOrderableChildren(true);
        List<ACE> entries = acl.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            ACE ace = entries.get(i);
            String nodeName = Util.generateAceName(ace, i);
            String ntName = (ace.isAllow()) ? NT_REP_GRANT_ACE : NT_REP_DENY_ACE;

            Tree aceNode = TreeUtil.addChild(aclTree, nodeName, ntName);
            aceNode.setProperty(REP_PRINCIPAL_NAME, ace.getPrincipal().getName());
            aceNode.setProperty(REP_PRIVILEGES, bitsProvider.getPrivilegeNames(ace.getPrivilegeBits()), Type.NAMES);
            Set<Restriction> restrictions = ace.getRestrictions();
            restrictionProvider.writeRestrictions(oakPath, aceNode, restrictions);
        }
    }

    @Override
    public void removePolicy(@Nullable String absPath, @NotNull AccessControlPolicy policy) throws RepositoryException {
        String oakPath = getOakPath(absPath);
        Util.checkValidPolicy(oakPath, policy);

        Map<String, Principal> principalMap = new HashMap<>();
        if (policy instanceof PrincipalACL) {
            PrincipalACL principalAcl = (PrincipalACL) policy;
            for (ACE ace : principalAcl.getEntries()) {
                String path = getNodePath(ace);
                Tree tree = getTree(path, Permissions.MODIFY_ACCESS_CONTROL, true);
                Tree aclTree = getAclTree(path, tree);
                if (aclTree == null) {
                    throw new AccessControlException("Unable to retrieve policy node at " + path);
                }
                Iterator<Tree> children = aclTree.getChildren().iterator();
                while (children.hasNext()) {
                    Tree child = children.next();
                    if (ace.equals(createACE(path, child, principalAcl.rProvider, principalMap))) {
                        child.remove();
                    }
                }
                if (!aclTree.getChildren().iterator().hasNext()) {
                    aclTree.remove();
                }
            }
        } else {
            Tree tree = getTree(oakPath, Permissions.MODIFY_ACCESS_CONTROL, true);
            Tree aclTree = getAclTree(oakPath, tree);
            if (aclTree != null) {
                aclTree.remove();
            } else {
                throw new AccessControlException("No policy to remove at " + absPath);
            }
        }
    }

    //-------------------------------------< JackrabbitAccessControlManager >---
    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(@NotNull Principal principal) throws RepositoryException {
        Util.checkValidPrincipal(principal, principalManager);

        String oakPath = (principal instanceof ItemBasedPrincipal) ? ((ItemBasedPrincipal) principal).getPath() : null;
        JackrabbitAccessControlPolicy policy = createPrincipalACL(oakPath, principal);

        if (policy != null) {
            return new JackrabbitAccessControlPolicy[0];
        } else {
            return new JackrabbitAccessControlPolicy[]{new PrincipalACL(oakPath, principal)};
        }
    }

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(@NotNull Principal principal) throws RepositoryException {
        Util.checkValidPrincipal(principal, principalManager);

        String oakPath = (principal instanceof ItemBasedPrincipal) ? ((ItemBasedPrincipal) principal).getPath() : null;
        JackrabbitAccessControlPolicy policy = createPrincipalACL(oakPath, principal);

        if (policy != null) {
            return new JackrabbitAccessControlPolicy[]{policy};
        } else {
            return new JackrabbitAccessControlPolicy[0];
        }
    }

    @NotNull
    @Override
    public AccessControlPolicy[] getEffectivePolicies(@NotNull Set<Principal> principals) throws RepositoryException {
        Util.checkValidPrincipals(principals, principalManager);
        Root r = getLatestRoot();

        Result aceResult = searchAces(principals, r);
        Set<JackrabbitAccessControlList> effective = Sets.newTreeSet(new AcListComparator());

        Set<String> paths = Sets.newHashSet();
        Predicate<Tree> predicate = new PrincipalPredicate(principals);
        for (ResultRow row : aceResult.getRows()) {
            String acePath = row.getPath();
            String aclName = Text.getName(Text.getRelativeParent(acePath, 1));

            Tree accessControlledTree = r.getTree(Text.getRelativeParent(acePath, 2));
            if (aclName.isEmpty() || !accessControlledTree.exists()) {
                log.debug("Isolated access control entry -> ignore query result at {}", acePath);
                continue;
            }

            String path = (REP_REPO_POLICY.equals(aclName)) ? null : accessControlledTree.getPath();
            if (paths.contains(path)) {
                continue;
            }
            JackrabbitAccessControlList policy = createACL(path, accessControlledTree, true, predicate);
            if (policy != null) {
                effective.add(policy);
                paths.add(path);
            }
        }
        return effective.toArray(new AccessControlPolicy[0]);
    }

    //--------------------------------------------------------< PolicyOwner >---
    @Override
    public boolean defines(String absPath, @NotNull AccessControlPolicy accessControlPolicy) {
        try {
            return Util.isValidPolicy(getOakPath(absPath), accessControlPolicy);
        } catch (RepositoryException e) {
            log.warn("Invalid absolute path '{}': {}", absPath, e.getMessage());
            return false;
        }
    }

    //------------------------------------------------------------< private >---
    @Nullable
    private Tree getAclTree(@Nullable String oakPath, @NotNull Tree accessControlledTree) {
        if (Util.isAccessControlled(oakPath, accessControlledTree, ntMgr)) {
            String aclName = Util.getAclName(oakPath);
            Tree policyTree = accessControlledTree.getChild(aclName);
            if (policyTree.exists()) {
                return policyTree;
            }
        }
        return null;
    }

    /**
     * @param oakPath the Oak path as specified with the ac mgr call.
     * @param tree    the access controlled node.
     * @return the new acl tree.
     * @throws AccessDeniedException In case the new acl tree is not accessible.
     */
    @NotNull
    private Tree createAclTree(@Nullable String oakPath, @NotNull Tree tree) throws AccessDeniedException {
        if (!Util.isAccessControlled(oakPath, tree, ntMgr)) {
            PropertyState mixins = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
            String mixinName = Util.getMixinName(oakPath);
            if (mixins == null) {
                tree.setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singleton(mixinName), Type.NAMES);
            } else {
                PropertyBuilder<String> pb = PropertyBuilder.copy(Type.NAME, mixins);
                pb.addValue(mixinName);
                tree.setProperty(pb.getPropertyState());
            }
        }
        String aclName = Util.getAclName(oakPath);
        return TreeUtil.addChild(tree, aclName, NT_REP_ACL);
    }

    @Nullable
    private JackrabbitAccessControlList createACL(@Nullable String oakPath,
                                                  @NotNull Tree accessControlledTree,
                                                  boolean isEffectivePolicy,
                                                  @NotNull Predicate<Tree> predicate) throws RepositoryException {
        if (!accessControlledTree.exists() || !Util.isAccessControlled(oakPath, accessControlledTree, ntMgr)) {
            return null;
        }

        Tree aclTree = accessControlledTree.getChild(Util.getAclName(oakPath));
        if (!aclTree.exists()) {
            return null;
        }

        List<ACE> entries = new ArrayList<>();
        Map<String, Principal> principalMap = new HashMap<>();
        for (Tree child : aclTree.getChildren()) {
            if (Util.isACE(child, ntMgr) && predicate.apply(child)) {
                ACE ace = createACE(oakPath, child, restrictionProvider, principalMap);
                entries.add(ace);
            }
        }
        if (!isEffectivePolicy) {
            return new NodeACL(oakPath, entries);
        } else {
            return (entries.isEmpty()) ? null : new ImmutableACL(oakPath, entries, restrictionProvider, getNamePathMapper());
        }
    }

    @Nullable
    private JackrabbitAccessControlList createPrincipalACL(@Nullable String oakPath,
                                                           @NotNull Principal principal) throws RepositoryException {
        Root root = getRoot();
        Result aceResult = searchAces(Collections.singleton(principal), root);
        RestrictionProvider restrProvider = new PrincipalRestrictionProvider(restrictionProvider);
        List<ACE> entries = new ArrayList<>();
        Map<String, Principal> principalMap = new HashMap<>();
        for (ResultRow row : aceResult.getRows()) {
            Tree aceTree = root.getTree(row.getPath());
            if (Util.isACE(aceTree, ntMgr)) {
                String aclPath = Text.getRelativeParent(aceTree.getPath(), 1);
                String path;
                if (aclPath.endsWith(REP_REPO_POLICY)) {
                    path = null;
                } else {
                    path = Text.getRelativeParent(aclPath, 1);
                }
                entries.add(createACE(path, aceTree, restrProvider, principalMap));
            }
        }
        if (entries.isEmpty()) {
            // nothing found
            return null;
        } else {
            return new PrincipalACL(oakPath, principal, entries, restrProvider);
        }
    }

    @NotNull
    private ACE createACE(@Nullable String oakPath,
                          @NotNull Tree aceTree,
                          @NotNull RestrictionProvider restrictionProvider,
                          @NotNull Map<String, Principal> principalMap) throws RepositoryException {
        boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(oakPath, aceTree);
        Iterable<String> privNames = checkNotNull(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
        return new Entry(getPrincipal(aceTree, principalMap), bitsProvider.getBits(privNames), isAllow, restrictions, getNamePathMapper());
    }

    @NotNull
    private static Result searchAces(@NotNull Set<Principal> principals, @NotNull Root root) throws RepositoryException {
        StringBuilder stmt = new StringBuilder(QueryConstants.SEARCH_ROOT_PATH);
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
        stmt.append(" order by jcr:path");

        try {
            QueryEngine queryEngine = root.getQueryEngine();
            return queryEngine.executeQuery(
                    stmt.toString(), Query.XPATH,
                    QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        } catch (ParseException e) {
            String msg = "Error while collecting effective policies.";
            log.error(msg, e);
            throw new RepositoryException(msg, e);
        }
    }

    @NotNull
    private Principal getPrincipal(@NotNull Tree aceTree, @NotNull Map<String, Principal> principalMap) {
        String principalName = checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
        return principalMap.computeIfAbsent(principalName, pn -> {
            Principal principal = principalManager.getPrincipal(pn);
            if (principal == null) {
                principal = new PrincipalImpl(pn);
            }
            return principal;
        });
    }

    private String getNodePath(ACE principalBasedAce) throws RepositoryException {
        Value v = principalBasedAce.getRestriction(REP_NODE_PATH);
        if (v == null) {
            throw new AccessControlException("Missing mandatory restriction rep:nodePath");
        } else {
            return getOakPath(Strings.emptyToNull(v.getString()));
        }
    }

    //--------------------------------------------------------------------------
    private class NodeACL extends ACL {

        NodeACL(@Nullable String oakPath) {
            this(oakPath, null);
        }

        NodeACL(@Nullable String oakPath, @Nullable List<ACE> entries) {
            super(oakPath, entries, AccessControlManagerImpl.this.getNamePathMapper());
        }

        @NotNull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }

        @Override
        ACE createACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) throws RepositoryException {
            return new Entry(principal, privilegeBits, isAllow, restrictions, getNamePathMapper());
        }

        @Override
        boolean checkValidPrincipal(Principal principal) throws AccessControlException {
            int importBehavior = Util.getImportBehavior(getConfig());
            if (!Util.checkValidPrincipal(principal, principalManager, importBehavior)) {
                return false;
            }

            if (PermissionUtil.isAdminOrSystem(ImmutableSet.of(principal), configParams)) {
                log.warn("Attempt to create an ACE for an administrative principal which always has full access: {}", getPath());
                switch (Util.getImportBehavior(getConfig())) {
                    case ImportBehavior.ABORT:
                        throw new AccessControlException("Attempt to create an ACE for an administrative principal which always has full access.");
                    case ImportBehavior.IGNORE:
                        return false;
                    case ImportBehavior.BESTEFFORT:
                        // just log warning, no other action required.
                        break;
                    default :
                        throw new IllegalArgumentException("Invalid import behavior" + importBehavior);
                }
            }
            return true;
        }

        @Override
        PrivilegeManager getPrivilegeManager() {
            return AccessControlManagerImpl.this.getPrivilegeManager();
        }

        @Override
        PrivilegeBits getPrivilegeBits(Privilege[] privileges) {
            return bitsProvider.getBits(privileges, getNamePathMapper());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof NodeACL) {
                NodeACL other = (NodeACL) obj;
                return Objects.equal(getOakPath(), other.getOakPath())
                        && getEntries().equals(other.getEntries());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private final class PrincipalACL extends ACL {

        private final Principal principal;
        private final RestrictionProvider rProvider;

        private PrincipalACL(@Nullable String oakPath, @NotNull Principal principal) {
            this(oakPath, principal, null, new PrincipalRestrictionProvider(restrictionProvider));
        }

        private PrincipalACL(@Nullable String oakPath, @NotNull Principal principal,
                             @Nullable List<ACE> entries,
                             @NotNull RestrictionProvider restrictionProvider) {
            super(oakPath, entries, AccessControlManagerImpl.this.getNamePathMapper());
            this.principal = principal;
            rProvider = restrictionProvider;
        }

        @NotNull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return rProvider;
        }

        @Override
        ACE createACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) throws RepositoryException {
            return new Entry(principal, privilegeBits, isAllow, restrictions, getNamePathMapper());
        }

        @Override
        boolean checkValidPrincipal(Principal principal) throws AccessControlException {
            Util.checkValidPrincipal(principal, principalManager);
            return true;
        }

        @Override
        PrivilegeManager getPrivilegeManager() {
            return AccessControlManagerImpl.this.getPrivilegeManager();
        }

        @Override
        PrivilegeBits getPrivilegeBits(Privilege[] privileges) {
            return bitsProvider.getBits(privileges, getNamePathMapper());
        }

        @Override
        public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws RepositoryException {
            throw new UnsupportedRepositoryOperationException("reordering is not supported");
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof PrincipalACL) {
                PrincipalACL other = (PrincipalACL) obj;
                return principal.equals(other.principal)
                        && Objects.equal(getOakPath(), other.getOakPath())
                        && getEntries().equals(other.getEntries());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private final class Entry extends ACE {

        private Entry(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions, NamePathMapper namePathMapper) throws AccessControlException {
            super(principal, privilegeBits, isAllow, restrictions, namePathMapper);
        }

        @Override
        public Privilege[] getPrivileges() {
            Set<Privilege> privileges = new HashSet<>();
            for (String name : bitsProvider.getPrivilegeNames(getPrivilegeBits())) {
                try {
                    privileges.add(getPrivilegeManager().getPrivilege(getNamePathMapper().getJcrName(name)));
                } catch (RepositoryException e) {
                    log.warn("Unable to get privilege with name : " + name, e);
                }
            }
            return privileges.toArray(new Privilege[0]);
        }
    }

    private static final class ReadPolicy implements NamedAccessControlPolicy {

        private static final NamedAccessControlPolicy INSTANCE = new ReadPolicy();

        private ReadPolicy() {
        }

        @Override
        public String getName() {
            return "Grants read access on configured trees.";
        }
    }

    private static final class PrincipalPredicate implements Predicate<Tree> {

        private final Iterable<String> principalNames;

        private PrincipalPredicate(@NotNull Set<Principal> principals) {
            principalNames = Iterables.transform(principals, Principal::getName);
        }

        @Override
        public boolean apply(@Nullable Tree aceTree) {
            return aceTree != null && Iterables.contains(principalNames, TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
        }
    }

    private static final class AcListComparator implements Comparator<JackrabbitAccessControlList> {
        @Override
        public int compare(JackrabbitAccessControlList list1, JackrabbitAccessControlList list2) {
            if (list1.equals(list2)) {
                return 0;
            } else {
                String p1 = list1.getPath();
                String p2 = list2.getPath();

                if (p1 == null) {
                    return -1;
                } else if (p2 == null) {
                    return 1;
                } else {
                    int depth1 = PathUtils.getDepth(p1);
                    int depth2 = PathUtils.getDepth(p2);
                    return (depth1 == depth2) ? p1.compareTo(p2) : Ints.compare(depth1, depth2);
                }

            }
        }
    }
}
