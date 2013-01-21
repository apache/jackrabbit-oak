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
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AccessControlValidator... TODO
 */
class AccessControlValidator implements Validator, AccessControlConstants {

    private final NodeUtil parentBefore;
    private final NodeUtil parentAfter;

    private final Map<String, PrivilegeDefinition> privilegeDefinitions;
    private final RestrictionProvider restrictionProvider;
    private final ReadOnlyNodeTypeManager ntMgr;

    AccessControlValidator(NodeUtil parentBefore, NodeUtil parentAfter,
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
        NodeUtil nodeAfter = checkNotNull(parentAfter.getChild(name));

        checkValidNode(parentAfter, nodeAfter);
        return new AccessControlValidator(null, nodeAfter, privilegeDefinitions, restrictionProvider, ntMgr);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        NodeUtil nodeBefore = checkNotNull(parentBefore.getChild(name));
        NodeUtil nodeAfter = checkNotNull(parentAfter.getChild(name));

        checkValidNode(parentAfter, nodeAfter);
        return new AccessControlValidator(nodeBefore, nodeAfter, privilegeDefinitions, restrictionProvider, ntMgr);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // TODO validate acl / ace / restriction removal
        return null;
    }

    //------------------------------------------------------------< private >---

    private void checkValidNode(NodeUtil parentAfter, NodeUtil nodeAfter) throws CommitFailedException {
        if (isPolicy(nodeAfter)) {
            checkValidPolicy(parentAfter, nodeAfter);
        } else if (isAccessControlEntry(nodeAfter)) {
            checkValidAccessControlEntry(nodeAfter);
        } else if (NT_REP_RESTRICTIONS.equals(nodeAfter.getPrimaryNodeTypeName())) {
            checkIsAccessControlEntry(parentAfter);
            checkValidRestrictions(parentAfter);
        }
    }

    private static boolean isPolicy(NodeUtil node) {
        return NT_REP_ACL.equals(node.getPrimaryNodeTypeName());
    }

    private static boolean isAccessControlEntry(NodeUtil node) {
        String ntName = node.getPrimaryNodeTypeName();
        return NT_REP_DENY_ACE.equals(ntName) || NT_REP_GRANT_ACE.equals(ntName);
    }

    private static void checkIsAccessControlEntry(NodeUtil node) throws CommitFailedException {
        if (!isAccessControlEntry(node)) {
            fail("Access control entry node expected.");
        }
    }

    private void checkValidPolicy(NodeUtil parent, NodeUtil policyNode) throws CommitFailedException {
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

    private void checkValidAccessControlledNode(NodeUtil accessControlledNode, String requiredMixin) throws CommitFailedException {
        Tree accessControlledTree = accessControlledNode.getTree();
        if (AC_NODETYPE_NAMES.contains(accessControlledNode.getPrimaryNodeTypeName())) {
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

    private void checkValidAccessControlEntry(NodeUtil aceNode) throws CommitFailedException {
        NodeUtil parent = aceNode.getParent();
        if (parent == null || !NT_REP_ACL.equals(parent.getPrimaryNodeTypeName())) {
            fail("Isolated access control entry at " + aceNode.getTree().getPath());
        }
        checkValidPrincipal(aceNode.getString(REP_PRINCIPAL_NAME, null));
        checkValidPrivileges(aceNode.getNames(REP_PRIVILEGES));
        checkValidRestrictions(aceNode);
    }

    private void checkValidPrincipal(String principalName) throws CommitFailedException {
        if (principalName == null || principalName.isEmpty()) {
            fail("Missing principal name.");
        }
        // TODO
        // if (!principalMgr.hasPrincipal(principal.getName())) {
        //     throw new AccessControlException("Principal " + principal.getName() + " does not exist.");
        // }
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

    private void checkValidRestrictions(NodeUtil aceNode) throws CommitFailedException {
        String path;
        NodeUtil aclNode = checkNotNull(aceNode.getParent());
        String aclPath = aclNode.getTree().getPath();
        if (REP_REPO_POLICY.equals(Text.getName(aclPath))) {
            path = null;
        } else {
            path = Text.getRelativeParent(aclPath, 1);
        }
        try {
            restrictionProvider.validateRestrictions(path, aceNode.getTree());
        } catch (AccessControlException e) {
            throw new CommitFailedException(e);
        }
    }


    private static void checkMixinTypes(NodeUtil parentNode) throws CommitFailedException {
        String[] mixinNames = parentNode.getNames(JcrConstants.JCR_MIXINTYPES);
        if (mixinNames != null && Arrays.asList(mixinNames).contains(MIX_REP_REPO_ACCESS_CONTROLLABLE)) {
            checkValidRepoAccessControlled(parentNode.getTree());
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
