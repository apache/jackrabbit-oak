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
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;

class PrincipalPolicyValidatorProvider extends ValidatorProvider implements Constants {

    private final MgrProvider mgrProvider;
    private final Set<Principal> principals;
    private final String workspaceName;

    private PermissionProvider permissionProvider;
    private TypePredicate isMixPrincipalBased;

    PrincipalPolicyValidatorProvider(@NotNull MgrProvider mgrProvider, @NotNull Set<Principal> principals, @NotNull String workspaceName) {
        this.mgrProvider = mgrProvider;
        this.principals = principals;
        this.workspaceName = workspaceName;
    }

    @Override
    protected PolicyValidator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        Root rootBefore = mgrProvider.getRootProvider().createReadOnlyRoot(before);
        permissionProvider = mgrProvider.getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getPermissionProvider(rootBefore, workspaceName, principals);
        isMixPrincipalBased = new TypePredicate(after, MIX_REP_PRINCIPAL_BASED_MIXIN);
        return new PolicyValidator(before, after);
    }

    private final class PolicyValidator extends DefaultValidator {

        private final Tree parentBefore;
        private final Tree parentAfter;
        private final boolean isNodetypeTree;

        private PolicyValidator(@NotNull NodeState rootStateBefore, @NotNull NodeState rootState) {
            mgrProvider.reset(mgrProvider.getRootProvider().createReadOnlyRoot(rootState), NamePathMapper.DEFAULT);
            this.parentBefore = mgrProvider.getTreeProvider().createReadOnlyTree(rootStateBefore);
            this.parentAfter = mgrProvider.getTreeProvider().createReadOnlyTree(rootState);
            this.isNodetypeTree = false;
        }

        private PolicyValidator(@NotNull PolicyValidator parentValidator, @NotNull Tree before, @NotNull Tree after) {
            this.parentBefore = before;
            this.parentAfter = after;
            if (parentValidator.isNodetypeTree) {
                this.isNodetypeTree = true;
            } else {
                this.isNodetypeTree = NodeTypeConstants.JCR_NODE_TYPES.equals(after.getName()) && JCR_SYSTEM.equals(parentValidator.getName());
            }
        }

        private PolicyValidator(@NotNull PolicyValidator parentValidator, @NotNull Tree tree, boolean isAfter) {
            this.parentBefore = (isAfter) ? null : tree;
            this.parentAfter = (isAfter) ? tree : null;
            if (parentValidator.isNodetypeTree) {
                this.isNodetypeTree = true;
            } else {
                this.isNodetypeTree = NodeTypeConstants.JCR_NODE_TYPES.equals(tree.getName()) && JCR_SYSTEM.equals(parentValidator.getName());
            }
        }

        @NotNull
        private String getName() {
            return (parentBefore == null) ? verifyNotNull(parentAfter).getName() : parentBefore.getName();
        }

        //------------------------------------------------------< Validator >---
        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            String propertyName = after.getName();
            if (JcrConstants.JCR_PRIMARYTYPE.equals(propertyName)) {
                if (NT_REP_PRINCIPAL_POLICY.equals(after.getValue(Type.NAME)) && !REP_PRINCIPAL_POLICY.equals(verifyNotNull(parentAfter).getName())) {
                    throw accessControlViolation(30, "Attempt create policy node with different name than '"+REP_PRINCIPAL_POLICY+"'.");
                }
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            String name = after.getName();
            if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
                if (NT_REP_PRINCIPAL_POLICY.equals(before.getValue(Type.STRING)) || NT_REP_PRINCIPAL_POLICY.equals(after.getValue(Type.STRING))) {
                    throw accessControlViolation(31, "Attempt to change primary type from/to rep:PrincipalPolicy.");
                }
            }
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            if (!isNodetypeTree) {
                if (REP_PRINCIPAL_POLICY.equals(name)) {
                    validatePolicyNode(verifyNotNull(parentAfter), after);
                } else if (REP_RESTRICTIONS.equals(name)) {
                    validateRestrictions(after);
                } else if (NT_REP_PRINCIPAL_ENTRY.equals(NodeStateUtils.getPrimaryTypeName(after))) {
                    validateEntry(name, after);
                }
            }
            return new VisibleValidator(nextValidator(name, after, true), true, true);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            if (!isNodetypeTree) {
                if (after.hasChildNode(REP_PRINCIPAL_POLICY)) {
                    Tree parent = mgrProvider.getTreeProvider().createReadOnlyTree(verifyNotNull(parentAfter), name, after);
                    validatePolicyNode(parent, after.getChildNode(REP_PRINCIPAL_POLICY));
                } else if (REP_RESTRICTIONS.equals(name)) {
                    validateRestrictions(after);
                } else if (NT_REP_PRINCIPAL_ENTRY.equals(NodeStateUtils.getPrimaryTypeName(after))) {
                    validateEntry(name, after);
                }
            }
            return new VisibleValidator(nextValidator(name, before, after), true, true);
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            if (!isNodetypeTree) {
                PropertyState effectivePath = null;
                if (REP_RESTRICTIONS.equals(name)) {
                    effectivePath = verifyNotNull(parentBefore).getProperty(REP_EFFECTIVE_PATH);
                } else if (NT_REP_PRINCIPAL_ENTRY.equals(NodeStateUtils.getPrimaryTypeName(before))) {
                    effectivePath = before.getProperty(REP_EFFECTIVE_PATH);
                }
                if (effectivePath != null && !Utils.hasModAcPermission(permissionProvider, effectivePath.getValue(Type.PATH))) {
                    throw new CommitFailedException(ACCESS, 3, "Access denied");
                }
            }
            return new VisibleValidator(nextValidator(name, before, false), true, true);        
        }

        //----------------------------------------------------------------------
        private void validatePolicyNode(@NotNull Tree parent, @NotNull NodeState nodeState) throws CommitFailedException {
            if (!NT_REP_PRINCIPAL_POLICY.equals(NodeStateUtils.getPrimaryTypeName(nodeState))) {
                throw accessControlViolation(32, "Reserved node name 'rep:principalPolicy' must only be used for nodes of type 'rep:PrincipalPolicy'.");
            }
            if (!isMixPrincipalBased.test(parent)) {
                throw accessControlViolation(33, "Parent node not of mixin type 'rep:PrincipalBasedMixin'.");
            }
        }

        private void validateRestrictions(@NotNull NodeState nodeState) throws CommitFailedException {
            if (!NT_REP_RESTRICTIONS.equals(NodeStateUtils.getPrimaryTypeName(nodeState))) {
                throw accessControlViolation(34, "Reserved node name 'rep:restrictions' must only be used for nodes of type 'rep:Restrictions'.");
            }
            Tree parent = verifyNotNull(parentAfter);
            if (NT_REP_PRINCIPAL_ENTRY.equals(TreeUtil.getPrimaryTypeName(parent))) {
                try {
                    String oakPath = Strings.emptyToNull(TreeUtil.getString(parent, REP_EFFECTIVE_PATH));
                    mgrProvider.getRestrictionProvider().validateRestrictions(oakPath, parent);
                } catch (AccessControlException e) {
                    throw new CommitFailedException(ACCESS_CONTROL, 35, "Invalid restrictions", e);
                } catch (RepositoryException e) {
                    throw new CommitFailedException(OAK, 13, "Internal error", e);
                }
            } else {
                // assert the restrictions node resides within access control content
                if (!mgrProvider.getContext().definesTree(parent)) {
                    throw new CommitFailedException(ACCESS_CONTROL, 2, "Expected access control entry parent (isolated restriction).");
                }
            }
        }

        private void validateEntry(@NotNull String name, @NotNull NodeState nodeState) throws CommitFailedException {
            Tree parent = verifyNotNull(parentAfter);
            String entryPath = PathUtils.concat(parent.getPath(), name);
            if (!REP_PRINCIPAL_POLICY.equals(parent.getName())) {
                throw accessControlViolation(36, "Isolated entry of principal policy at " + entryPath);
            }
            Iterable<String> privilegeNames = nodeState.getNames(REP_PRIVILEGES);
            if (Iterables.isEmpty(privilegeNames)) {
                throw accessControlViolation(37, "Empty rep:privileges property at " + entryPath);
            }
            PrivilegeManager privilegeManager = mgrProvider.getPrivilegeManager();
            for (String privilegeName : privilegeNames) {
                try {
                    Privilege privilege = privilegeManager.getPrivilege(privilegeName);
                    if (privilege.isAbstract()) {
                        throw accessControlViolation(38, "Abstract privilege " + privilegeName + " at " + entryPath);
                    }
                } catch (AccessControlException e) {
                    throw accessControlViolation(39, "Invalid privilege " + privilegeName + " at " + entryPath);
                } catch (RepositoryException e) {
                    throw new CommitFailedException(OAK, 13, "Internal error", e);
                }
            }
            // check mod-access-control permission on the effective path
            PropertyState effectivePath = nodeState.getProperty(REP_EFFECTIVE_PATH);
            if (effectivePath == null) {
                throw new CommitFailedException(CONSTRAINT, 21, "Missing mandatory rep:effectivePath property at " + entryPath);
            }
            if (!Utils.hasModAcPermission(permissionProvider, effectivePath.getValue(Type.PATH))) {
                throw new CommitFailedException(ACCESS, 3, "Access denied");
            }
        }

        private CommitFailedException accessControlViolation(int code, String message) {
            return new CommitFailedException(ACCESS_CONTROL, code, message);
        }

        private PolicyValidator nextValidator(@NotNull String name, @NotNull NodeState beforeState, @NotNull NodeState afterState) {
            Tree before = mgrProvider.getTreeProvider().createReadOnlyTree(verifyNotNull(parentBefore), name, beforeState);
            Tree after = mgrProvider.getTreeProvider().createReadOnlyTree(verifyNotNull(parentAfter), name, afterState);
            return new PolicyValidator(this, before, after);
        }

        private PolicyValidator nextValidator(@NotNull String name, @NotNull NodeState nodeState, boolean isAfter) {
            Tree parent = (isAfter) ? parentAfter : parentBefore;
            Tree tree = mgrProvider.getTreeProvider().createReadOnlyTree(verifyNotNull(parent), name, nodeState);
            return new PolicyValidator(this, tree, isAfter);
        }

        @NotNull
        private Tree verifyNotNull(@Nullable Tree tree) {
            Validate.checkState(tree != null);
            return tree;
        }
    }
}
