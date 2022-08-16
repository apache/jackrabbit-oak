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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.findGroupIdInHierarchy;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.isGroup;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.isMemberProperty;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.isMembersType;

class DynamicGroupValidatorProvider extends ValidatorProvider implements ExternalIdentityConstants {
    
    private final RootProvider rootProvider;
    private final TreeProvider treeProvider;
    private final Set<String> idpNamesWithDynamicGroups;
    private final String groupRootPath;

    private Root rootBefore;
    private Root rootAfter;

    DynamicGroupValidatorProvider(@NotNull RootProvider rootProvider,
                                  @NotNull TreeProvider treeProvider,
                                  @NotNull SecurityProvider securityProvider,
                                  @NotNull Set<String> idpNamesWithDynamicGroups) {
        this.rootProvider = rootProvider;
        this.treeProvider = treeProvider;
        this.idpNamesWithDynamicGroups = idpNamesWithDynamicGroups;

        this.groupRootPath = checkNotNull(UserUtil.getAuthorizableRootPath(securityProvider.getParameters(UserConfiguration.NAME), AuthorizableType.GROUP));
    }

    @Override
    protected @NotNull Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        if (idpNamesWithDynamicGroups.isEmpty()) {
            return DefaultValidator.INSTANCE;
        }
        
        this.rootBefore = rootProvider.createReadOnlyRoot(before);
        this.rootAfter = rootProvider.createReadOnlyRoot(after);
        
        return new SubtreeValidator(new DynamicGroupValidator(), Iterables.toArray(PathUtils.elements(groupRootPath), String.class));
    }
    
    private class DynamicGroupValidator extends DefaultValidator {

        private Tree parentBefore;
        private Tree parentAfter;

        boolean isDynamicGroup = false;

        private DynamicGroupValidator() {}

        private DynamicGroupValidator(@NotNull Tree parentBefore, @NotNull Tree parentAfter, boolean isDynamicGroup) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
            this.isDynamicGroup = isDynamicGroup;
        }

        private DynamicGroupValidator(@NotNull Tree parentAfter, boolean isDynamicGroup) {
            this.parentAfter = parentAfter;
            this.isDynamicGroup = isDynamicGroup;
        }
        
        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            if (isDynamicGroup && isMemberProperty(after)) {
                throw commitFailedException(getParentAfter());
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            if (isDynamicGroup && isMemberProperty(before)) {
                Set<String> refsBefore = Sets.newHashSet(before.getValue(Type.STRINGS));
                Set<String> refsAfter = Sets.newHashSet(after.getValue(Type.STRINGS));
                refsAfter.removeAll(refsBefore);
                if (!refsAfter.isEmpty()) {
                    throw commitFailedException(getParentBefore());
                }
            }
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            Tree afterTree = treeProvider.createReadOnlyTree(getParentAfter(), name, after);
            boolean dynamicGroupChild = isDynamicGroup(this, afterTree);

            if (dynamicGroupChild) {
                if (isMembersType(afterTree)) {
                    throw commitFailedException(getParentAfter());
                }
                return new DynamicGroupValidator(afterTree, true);
            } else if (!isGroup(afterTree)) {
                return new DynamicGroupValidator(afterTree, false);
            } else {
                // a regular group -> no need to traverse into the subtree
                return null;
            }
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) {
            Tree beforeTree = treeProvider.createReadOnlyTree(getParentBefore(), name, before);
            Tree afterTree = treeProvider.createReadOnlyTree(getParentAfter(), name, after);

            boolean dynamicGroupChild = isDynamicGroup(this, beforeTree);
            if (dynamicGroupChild || !isGroup(beforeTree)) {
                return new DynamicGroupValidator(beforeTree, afterTree, dynamicGroupChild);
            } else {
                // no need to traverse into a regular group
                return null;
            }
        }
        
        private boolean isDynamicGroup(@NotNull DynamicGroupValidator parentValidator, @NotNull Tree tree) {
            if (parentValidator.isDynamicGroup) {
                return true;
            } else {
                return isDynamicGroup(tree);
            }
        }
        
        private boolean isDynamicGroup(@NotNull Tree tree) {
            if (UserUtil.isType(tree, AuthorizableType.GROUP)) {
                PropertyState ps = tree.getProperty(REP_EXTERNAL_ID);
                if (ps == null) {
                    return false;
                }
                String providerName = ExternalIdentityRef.fromString(ps.getValue(Type.STRING)).getProviderName();
                return providerName != null && idpNamesWithDynamicGroups.contains(providerName);
            } 
            return false;
        }
        
        private @NotNull Tree getParentBefore() {
            if (parentBefore == null) {
                parentBefore = rootBefore.getTree(groupRootPath);
            }
            return parentBefore;
        }

        private @NotNull Tree getParentAfter() {
            if (parentAfter == null) {
                parentAfter = rootAfter.getTree(groupRootPath);
            }
            return parentAfter;
        }

        private @NotNull CommitFailedException commitFailedException(@NotNull Tree tree) {
            String msg = String.format("Attempt to add members to dynamic group '%s' at '%s'", findGroupIdInHierarchy(tree), tree.getPath());
            return new CommitFailedException(CommitFailedException.CONSTRAINT, 77, msg);
        }
    }
}