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

import java.util.Map;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * AccessControlValidator... TODO
 */
class AccessControlValidator implements Validator, AccessControlConstants {

    private final NodeUtil parentBefore;
    private final NodeUtil parentAfter;
    private final Map<String, PrivilegeDefinition> privilegeDefinitions;
    private final RestrictionProvider restrictionProvider;

    AccessControlValidator(NodeUtil parentBefore, NodeUtil parentAfter,
                           Map<String, PrivilegeDefinition> privilegeDefinitions,
                           RestrictionProvider restrictionProvider) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.privilegeDefinitions = privilegeDefinitions;
        this.restrictionProvider = restrictionProvider;
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // TODO: validate access control property
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // TODO: validate access control property
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // nothing to do: mandatory properties will be enforced by node type validator
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        NodeUtil node = parentAfter.getChild(name);
        if (isAccessControlEntry(node)) {
            checkValidAccessControlEntry(node);
            return null;
        } else {
            return new AccessControlValidator(null, node, privilegeDefinitions, restrictionProvider);
        }
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        NodeUtil nodeBefore = parentBefore.getChild(name);
        NodeUtil nodeAfter = parentAfter.getChild(name);
        if (isAccessControlEntry(nodeAfter)) {
            checkValidAccessControlEntry(nodeAfter);
            return null;
        } else {
            return new AccessControlValidator(nodeBefore, nodeAfter, privilegeDefinitions, restrictionProvider);
        }
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // TODO validate acl / ace / restriction removal
        return null;
    }

    //------------------------------------------------------------< private >---
    private boolean isAccessControlEntry(NodeUtil node) {
        String ntName = node.getPrimaryNodeTypeName();
        return NT_REP_DENY_ACE.equals(ntName) || NT_REP_GRANT_ACE.equals(ntName);
    }

    private void checkValidAccessControlEntry(NodeUtil aceNode) throws CommitFailedException {
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
            if (!privilegeDefinitions.containsKey(privilegeName)) {
                fail("Unknown privilege " + privilegeName);
            }

            PrivilegeDefinition def = privilegeDefinitions.get(privilegeName);
            if (def.isAbstract()) {
                fail("Abstract privilege " + privilegeName);
            }
        }
    }

    private void checkValidRestrictions(NodeUtil aceNode) throws CommitFailedException {
        try {
            String path = null; // TODO
            restrictionProvider.validateRestrictions(path, aceNode.getTree());
        } catch (AccessControlException e) {
            throw new CommitFailedException(e);
        }
    }

    private static void fail(String msg) throws CommitFailedException {
        throw new CommitFailedException(new AccessControlException(msg));
    }
}
