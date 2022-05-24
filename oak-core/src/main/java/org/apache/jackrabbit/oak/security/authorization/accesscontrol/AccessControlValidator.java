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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;

/**
 * Validation for access control information changed by regular JCR (and Jackrabbit)
 * access control management API.
 */
class AccessControlValidator extends DefaultValidator implements AccessControlConstants {

    private final TreeProvider treeProvider;
    private final Tree parentAfter;

    private final PrivilegeBitsProvider privilegeBitsProvider;
    private final PrivilegeManager privilegeManager;
    private final RestrictionProvider restrictionProvider;

    private final TypePredicate isRepoAccessControllable;
    private final TypePredicate isAccessControllable;

    private final Context ctx;

    AccessControlValidator(@NotNull NodeState parentAfter,
                           @NotNull PrivilegeManager privilegeManager,
                           @NotNull PrivilegeBitsProvider privilegeBitsProvider,
                           @NotNull RestrictionProvider restrictionProvider,
                           @NotNull ProviderCtx providerCtx) {
        treeProvider = providerCtx.getTreeProvider();
        this.parentAfter = treeProvider.createReadOnlyTree(parentAfter);
        this.privilegeBitsProvider = privilegeBitsProvider;
        this.privilegeManager = privilegeManager;
        this.restrictionProvider = restrictionProvider;
        this.isRepoAccessControllable = new TypePredicate(parentAfter, MIX_REP_REPO_ACCESS_CONTROLLABLE);
        this.isAccessControllable = new TypePredicate(parentAfter, MIX_REP_ACCESS_CONTROLLABLE);
        ctx = providerCtx.getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getContext();
    }

    private AccessControlValidator(AccessControlValidator parent, Tree parentAfter) {
        this.treeProvider = parent.treeProvider;
        this.parentAfter = parentAfter;
        this.privilegeBitsProvider = parent.privilegeBitsProvider;
        this.privilegeManager = parent.privilegeManager;
        this.restrictionProvider = parent.restrictionProvider;
        this.isRepoAccessControllable = parent.isRepoAccessControllable;
        this.isAccessControllable = parent.isAccessControllable;
        this.ctx = parent.ctx;
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (isAccessControlEntry(parentAfter)) {
            checkValidAccessControlEntry(parentAfter);
        }
        if (JcrConstants.JCR_MIXINTYPES.equals(after.getName())) {
            checkMixinTypes(parentAfter);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (isAccessControlEntry(parentAfter)) {
            checkValidAccessControlEntry(parentAfter);
        }
        if (JcrConstants.JCR_MIXINTYPES.equals(after.getName())) {
            checkMixinTypes(parentAfter);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        // nothing to do: mandatory properties will be enforced by node type validator
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        Tree treeAfter = checkNotNull(parentAfter.getChild(name));

        checkValidTree(parentAfter, treeAfter, after);
        return newValidator(this, treeAfter);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Tree treeAfter = checkNotNull(parentAfter.getChild(name));

        checkValidTree(parentAfter, treeAfter, after);
        return newValidator(this, treeAfter);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        // nothing to do
        return null;
    }

    //------------------------------------------------------------< private >---

    private static Validator newValidator(AccessControlValidator parent,
                                          Tree parentAfter) {
        return new VisibleValidator(
                new AccessControlValidator(parent, parentAfter),
                true,
                true);
    }

    private void checkValidTree(Tree parentAfter, Tree treeAfter, NodeState nodeAfter) throws CommitFailedException {
        if (isPolicy(treeAfter)) {
            checkValidPolicy(parentAfter, treeAfter, nodeAfter);
        } else if (isAccessControlEntry(treeAfter)) {
            checkValidAccessControlEntry(treeAfter);
        } else if (NT_REP_RESTRICTIONS.equals(TreeUtil.getPrimaryTypeName(treeAfter))) {
            // only validate restrictions if defined with an ACE controlled by this validator. otherwise verify that
            // the parent is indeed access control content as defined by the authorization Context.
            // this allows alternative authorization models to re-use the configured restriction provide and store
            // them in a 'rep:Restriction' node even if the surrounding policy/access control entries look different
            if (isAccessControlEntry(parentAfter)) {
                checkValidRestrictions(parentAfter);
            } else if (!ctx.definesTree(parentAfter)) {
                throw accessViolation(2, "Access control entry node expected at " + parentAfter.getPath());
            }
        }
    }

    private static boolean isPolicy(Tree tree) {
        return NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private static boolean isAccessControlEntry(Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        return NT_REP_DENY_ACE.equals(ntName) || NT_REP_GRANT_ACE.equals(ntName);
    }

    private void checkValidPolicy(Tree parent, Tree policyTree, NodeState policyNode) throws CommitFailedException {
        if (REP_REPO_POLICY.equals(policyTree.getName())) {
            checkValidAccessControlledNode(parent, isRepoAccessControllable, treeProvider);
            checkValidRepoAccessControlled(parent);
        } else {
            checkValidAccessControlledNode(parent, isAccessControllable, treeProvider);
        }

        Collection<String> validPolicyNames = (parent.isRoot()) ?
                POLICY_NODE_NAMES :
                Collections.singleton(REP_POLICY);
        if (!validPolicyNames.contains(policyTree.getName())) {
            throw accessViolation(3, "Invalid policy name " + policyTree.getName() + " at " + parent.getPath());
        }

        if (!policyNode.hasProperty(TreeConstants.OAK_CHILD_ORDER)) {
            throw accessViolation(4, "Invalid policy node at " + policyTree.getPath() + ": Order of children is not stable.");
        }

        Set<ValidationEntry> aceSet = Sets.newHashSet();
        for (Tree child : policyTree.getChildren()) {
            if (isAccessControlEntry(child)) {
                ValidationEntry entry = createAceEntry(parent.getPath(), child);
                if (!aceSet.add(entry)) { 
                    String restrs = String.join(", ", entry.restrictions.stream().map(restriction -> restriction.getProperty().toString()).collect(Collectors.toSet()));
                    String msg = String.format("Duplicate ACE '%s' found in policy. (principal = %s, isallow = %b, privileges = %s, restrictions = [%s])", child.getPath(), entry.principalName, entry.isAllow, privilegeBitsProvider.getPrivilegeNames(entry.privilegeBits), restrs);
                    throw accessViolation(13, msg);
                }
            }
        }
    }

    private static void checkValidAccessControlledNode(@NotNull Tree accessControlledTree,
                                                       @NotNull TypePredicate requiredMixin,
                                                       @NotNull TreeProvider treeProvider) throws CommitFailedException {
        if (AC_NODETYPE_NAMES.contains(TreeUtil.getPrimaryTypeName(accessControlledTree))) {
            throw accessViolation(5, "Access control policy within access control content (" + accessControlledTree.getPath() + ')');
        }

        if (!requiredMixin.test(treeProvider.asNodeState(accessControlledTree))) {
            String msg = "Isolated policy node (" + accessControlledTree.getPath() + "). Parent is not of type " + requiredMixin;
            throw accessViolation(6, msg);
        }
    }

    private void checkValidAccessControlEntry(@NotNull Tree aceNode) throws CommitFailedException {
        Tree parent = aceNode.getParent();
        if (!NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(parent))) {
            throw accessViolation(7, "Isolated access control entry at " + aceNode.getPath());
        }
        checkValidPrincipal(aceNode);
        checkValidPrivileges(aceNode);
        checkValidRestrictions(aceNode);
    }

    @NotNull
    private static String checkValidPrincipal(@NotNull Tree aceNode) throws CommitFailedException {
        String principalName = TreeUtil.getString(aceNode, REP_PRINCIPAL_NAME);
        if (Strings.isNullOrEmpty(principalName)) {
            throw accessViolation(8, "Missing principal name at " + aceNode.getPath());
        }
        // validity of principal is only a JCR specific contract and will not be
        // enforced on the oak level.
        return principalName;
    }

    private void checkValidPrivileges(@NotNull Tree aceNode) throws CommitFailedException {
        Iterable<String> privilegeNames = getPrivilegeNames(aceNode);
        for (String privilegeName : privilegeNames) {
            try {
                Privilege privilege = privilegeManager.getPrivilege(privilegeName);
                if (privilege.isAbstract()) {
                    throw accessViolation(11, "Abstract privilege " + privilegeName + " at " + aceNode.getPath());
                }
            } catch (AccessControlException e) {
                throw accessViolation(10, "Invalid privilege " + privilegeName + " at " + aceNode.getPath());
            } catch (RepositoryException e) {
                throw new IllegalStateException("Failed to read privileges", e);
            }
        }
    }

    @NotNull
    private static Iterable<String> getPrivilegeNames(@NotNull Tree aceNode) throws CommitFailedException {
        Iterable<String> privilegeNames = TreeUtil.getNames(aceNode, REP_PRIVILEGES);
        if (Iterables.isEmpty(privilegeNames)) {
            throw accessViolation(9, "Missing privileges at " + aceNode.getPath());
        }
        return privilegeNames;
    }

    private void checkValidRestrictions(@NotNull Tree aceTree) throws CommitFailedException {
        String path;
        Tree aclTree = checkNotNull(aceTree.getParent());
        String aclPath = aclTree.getPath();
        if (REP_REPO_POLICY.equals(Text.getName(aclPath))) {
            path = null;
        } else {
            path = Text.getRelativeParent(aclPath, 1);
        }
        try {
            restrictionProvider.validateRestrictions(path, aceTree);
        } catch (AccessControlException e) {
            throw new CommitFailedException(ACCESS_CONTROL, 1, "Access control violation", e);
        } catch (RepositoryException e) {
            throw new CommitFailedException(OAK, 13, "Internal error", e);
        }
    }


    private static void checkMixinTypes(Tree parentTree) throws CommitFailedException {
        Iterable<String> mixinNames = TreeUtil.getNames(parentTree, JcrConstants.JCR_MIXINTYPES);
        if (Iterables.contains(mixinNames, MIX_REP_REPO_ACCESS_CONTROLLABLE)) {
            checkValidRepoAccessControlled(parentTree);
        }
    }

    private static void checkValidRepoAccessControlled(@NotNull Tree accessControlledTree) throws CommitFailedException {
        if (!accessControlledTree.isRoot()) {
            throw accessViolation(12, "Only root can store repository level policies (" + accessControlledTree.getPath() + ')');
        }
    }

    @NotNull
    private static CommitFailedException accessViolation(int code, String message) {
        return new CommitFailedException(ACCESS_CONTROL, code, message);
    }

    @NotNull
    private ValidationEntry createAceEntry(@Nullable String path, @NotNull Tree aceTree) throws CommitFailedException {
        String principalName = checkValidPrincipal(aceTree);
        PrivilegeBits privilegeBits = privilegeBitsProvider.getBits(getPrivilegeNames(aceTree));
        boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(path, aceTree);
        return new ValidationEntry(principalName, privilegeBits, isAllow, restrictions);
    }
}
