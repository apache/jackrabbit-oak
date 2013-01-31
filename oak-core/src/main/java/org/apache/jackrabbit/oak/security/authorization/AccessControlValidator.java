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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AccessControlValidator... TODO
 */
class AccessControlValidator implements Validator, AccessControlConstants {

    private final Tree parentBefore;
    private final Tree parentAfter;

    private final Map<String, PrivilegeDefinition> privilegeDefinitions;
    private final RestrictionProvider restrictionProvider;
    private final ReadOnlyNodeTypeManager ntMgr;

    AccessControlValidator(Tree parentBefore, Tree parentAfter,
                           Map<String, PrivilegeDefinition> privilegeDefinitions,
                           RestrictionProvider restrictionProvider, ReadOnlyNodeTypeManager ntMgr) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.privilegeDefinitions = privilegeDefinitions;
        this.restrictionProvider = restrictionProvider;
        this.ntMgr = ntMgr;
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

        checkValidTree(parentAfter, treeAfter);
        return new AccessControlValidator(null, treeAfter, privilegeDefinitions, restrictionProvider, ntMgr);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Tree treeBefore = checkNotNull(parentBefore.getChild(name));
        Tree treeAfter = checkNotNull(parentAfter.getChild(name));

        checkValidTree(parentAfter, treeAfter);
        return new AccessControlValidator(treeBefore, treeAfter, privilegeDefinitions, restrictionProvider, ntMgr);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // TODO validate acl / ace / restriction removal
        return null;
    }

    //------------------------------------------------------------< private >---

    private void checkValidTree(Tree parentAfter, Tree treeAfter) throws CommitFailedException {
        if (isPolicy(treeAfter)) {
            checkValidPolicy(parentAfter, treeAfter);
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
            fail("Access control entry node expected.");
        }
    }

    private void checkValidPolicy(Tree parent, Tree policyNode) throws CommitFailedException {
        String mixinType = (REP_REPO_POLICY.equals(policyNode.getName())) ?
                MIX_REP_REPO_ACCESS_CONTROLLABLE :
                MIX_REP_ACCESS_CONTROLLABLE;
        checkValidAccessControlledNode(parent, mixinType);

        Collection<String> validPolicyNames = (parent.isRoot()) ?
                POLICY_NODE_NAMES :
                Collections.singleton(REP_POLICY);
        if (!validPolicyNames.contains(policyNode.getName())) {
            fail("Invalid policy name " + policyNode.getName());
        }
    }

    private void checkValidAccessControlledNode(Tree accessControlledTree, String requiredMixin) throws CommitFailedException {
        if (AC_NODETYPE_NAMES.contains(TreeUtil.getPrimaryTypeName(accessControlledTree))) {
            fail("Access control policy within access control content (" + accessControlledTree.getPath() + ')');
        }

        String msg = "Isolated policy node. Parent is not of type " + requiredMixin;
        try {
            if (!ntMgr.isNodeType(accessControlledTree, requiredMixin)) {
                fail(msg);
            }
        } catch (RepositoryException e) {
            throw new CommitFailedException(msg, e);
        }

        if (MIX_REP_REPO_ACCESS_CONTROLLABLE.equals(requiredMixin)) {
            checkValidRepoAccessControlled(accessControlledTree);
        }
    }

    private void checkValidAccessControlEntry(Tree aceNode) throws CommitFailedException {
        Tree parent = aceNode.getParent();
        if (parent == null || !NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(parent))) {
            fail("Isolated access control entry at " + aceNode.getPath());
        }
        checkValidPrincipal(TreeUtil.getString(aceNode, REP_PRINCIPAL_NAME));
        checkValidPrivileges(TreeUtil.getStrings(aceNode, REP_PRIVILEGES));
        checkValidRestrictions(aceNode);
    }

    private void checkValidPrincipal(String principalName) throws CommitFailedException {
        if (principalName == null || principalName.isEmpty()) {
            fail("Missing principal name.");
        }
        // validity of principal is only a JCR specific contract and will not be
        // enforced on the oak level.
    }

    private void checkValidPrivileges(String[] privilegeNames) throws CommitFailedException {
        if (privilegeNames == null || privilegeNames.length == 0) {
            fail("Missing privileges.");
        }
        for (String privilegeName : privilegeNames) {
            if (privilegeName == null || !privilegeDefinitions.containsKey(privilegeName)) {
                fail("Invalid privilege " + privilegeName);
            }

            PrivilegeDefinition def = privilegeDefinitions.get(privilegeName);
            if (def.isAbstract()) {
                fail("Abstract privilege " + privilegeName);
            }
        }
    }

    private void checkValidRestrictions(Tree aceTree) throws CommitFailedException {
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
            throw new CommitFailedException(e);
        }
    }


    private static void checkMixinTypes(Tree parentTree) throws CommitFailedException {
        String[] mixinNames = TreeUtil.getStrings(parentTree, JcrConstants.JCR_MIXINTYPES);
        if (mixinNames != null && Arrays.asList(mixinNames).contains(MIX_REP_REPO_ACCESS_CONTROLLABLE)) {
            checkValidRepoAccessControlled(parentTree);
        }
    }

    private static void checkValidRepoAccessControlled(Tree accessControlledTree) throws CommitFailedException {
        if (!accessControlledTree.isRoot()) {
            fail("Only root can store repository level policies.");
        }
    }

    private static void fail(String msg) throws CommitFailedException {
        throw new CommitFailedException(new AccessControlException(msg));
    }
}
