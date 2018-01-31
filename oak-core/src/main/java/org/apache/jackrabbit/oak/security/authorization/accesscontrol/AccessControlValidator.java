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
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;

/**
 * Validation for access control information changed by regular JCR (and Jackrabbit)
 * access control management API.
 */
class AccessControlValidator extends DefaultValidator implements AccessControlConstants {

    private final Tree parentAfter;

    private final PrivilegeBitsProvider privilegeBitsProvider;
    private final PrivilegeManager privilegeManager;
    private final RestrictionProvider restrictionProvider;

    private final TypePredicate isRepoAccessControllable;
    private final TypePredicate isAccessControllable;

    AccessControlValidator(@Nonnull NodeState parentAfter,
                           @Nonnull PrivilegeManager privilegeManager,
                           @Nonnull PrivilegeBitsProvider privilegeBitsProvider,
                           @Nonnull RestrictionProvider restrictionProvider,
                           @Nonnull TreeProvider treeProvider) {
        this.parentAfter = treeProvider.createReadOnlyTree(parentAfter);
        this.privilegeBitsProvider = privilegeBitsProvider;
        this.privilegeManager = privilegeManager;
        this.restrictionProvider = restrictionProvider;
        this.isRepoAccessControllable = new TypePredicate(parentAfter, MIX_REP_REPO_ACCESS_CONTROLLABLE);
        this.isAccessControllable = new TypePredicate(parentAfter, MIX_REP_ACCESS_CONTROLLABLE);
    }

    private AccessControlValidator(AccessControlValidator parent, Tree parentAfter) {
        this.parentAfter = parentAfter;
        this.privilegeBitsProvider = parent.privilegeBitsProvider;
        this.privilegeManager = parent.privilegeManager;
        this.restrictionProvider = parent.restrictionProvider;
        this.isRepoAccessControllable = parent.isRepoAccessControllable;
        this.isAccessControllable = parent.isAccessControllable;
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
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
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
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
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
            checkIsAccessControlEntry(parentAfter);
            checkValidRestrictions(parentAfter);
        }
    }

    private static boolean isPolicy(Tree tree) {
        return NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private static boolean isAccessControlEntry(Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        return NT_REP_DENY_ACE.equals(ntName) || NT_REP_GRANT_ACE.equals(ntName);
    }

    private static void checkIsAccessControlEntry(Tree tree) throws CommitFailedException {
        if (!isAccessControlEntry(tree)) {
            throw accessViolation(2, "Access control entry node expected at " + tree.getPath());
        }
    }

    private void checkValidPolicy(Tree parent, Tree policyTree, NodeState policyNode) throws CommitFailedException {
        if (REP_REPO_POLICY.equals(policyTree.getName())) {
            checkValidAccessControlledNode(parent, isRepoAccessControllable);
            checkValidRepoAccessControlled(parent);
        } else {
            checkValidAccessControlledNode(parent, isAccessControllable);
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

        Set<Entry> aceSet = Sets.newHashSet();
        for (Tree child : policyTree.getChildren()) {
            if (isAccessControlEntry(child)) {
                if (!aceSet.add(new Entry(parent.getPath(), child))) {
                    throw accessViolation(13, "Duplicate ACE '" + child.getPath() + "' found in policy");
                }
            }
        }
    }

    private static void checkValidAccessControlledNode(@Nonnull Tree accessControlledTree,
                                                       @Nonnull TypePredicate requiredMixin) throws CommitFailedException {
        if (AC_NODETYPE_NAMES.contains(TreeUtil.getPrimaryTypeName(accessControlledTree))) {
            throw accessViolation(5, "Access control policy within access control content (" + accessControlledTree.getPath() + ')');
        }

        NodeState ns = (accessControlledTree instanceof AbstractTree) ? ((AbstractTree) accessControlledTree).getNodeState() : null;
        if (!requiredMixin.apply(ns)) {
            String msg = "Isolated policy node (" + accessControlledTree.getPath() + "). Parent is not of type " + requiredMixin;
            throw accessViolation(6, msg);
        }
    }

    private void checkValidAccessControlEntry(@Nonnull Tree aceNode) throws CommitFailedException {
        Tree parent = aceNode.getParent();
        if (!parent.exists() || !NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(parent))) {
            throw accessViolation(7, "Isolated access control entry at " + aceNode.getPath());
        }
        checkValidPrincipal(aceNode);
        checkValidPrivileges(aceNode);
        checkValidRestrictions(aceNode);
    }

    private void checkValidPrincipal(@Nonnull Tree aceNode) throws CommitFailedException {
        String principalName = TreeUtil.getString(aceNode, REP_PRINCIPAL_NAME);
        if (principalName == null || principalName.isEmpty()) {
            throw accessViolation(8, "Missing principal name at " + aceNode.getPath());
        }
        // validity of principal is only a JCR specific contract and will not be
        // enforced on the oak level.
    }

    private void checkValidPrivileges(@Nonnull Tree aceNode) throws CommitFailedException {
        Iterable<String> privilegeNames = TreeUtil.getStrings(aceNode, REP_PRIVILEGES);
        if (privilegeNames == null || Iterables.isEmpty(privilegeNames)) {
            throw accessViolation(9, "Missing privileges at " + aceNode.getPath());
        }
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

    private void checkValidRestrictions(@Nonnull Tree aceTree) throws CommitFailedException {
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

    private static void checkValidRepoAccessControlled(Tree accessControlledTree) throws CommitFailedException {
        if (!accessControlledTree.isRoot()) {
            throw accessViolation(12, "Only root can store repository level policies (" + accessControlledTree.getPath() + ')');
        }
    }

    private static CommitFailedException accessViolation(int code, String message) {
        return new CommitFailedException(ACCESS_CONTROL, code, message);
    }

    private final class Entry {

        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final Set<Restriction> restrictions;

        private Entry(String path, Tree aceTree) {
            principalName = aceTree.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING);
            privilegeBits = privilegeBitsProvider.getBits(aceTree.getProperty(REP_PRIVILEGES).getValue(Type.NAMES));
            restrictions = restrictionProvider.readRestrictions(path, aceTree);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(principalName, privilegeBits, restrictions);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof Entry) {
                Entry other = (Entry) o;
                return Objects.equal(principalName, other.principalName)
                        && privilegeBits.equals(other.privilegeBits)
                        && restrictions.equals(other.restrictions);
            }
            return false;
        }
    }
}
