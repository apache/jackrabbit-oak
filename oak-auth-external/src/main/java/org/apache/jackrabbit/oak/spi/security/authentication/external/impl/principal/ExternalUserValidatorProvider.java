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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;

class ExternalUserValidatorProvider extends ValidatorProvider implements ExternalIdentityConstants  {

    private static final Logger log = LoggerFactory.getLogger(ExternalUserValidatorProvider.class);

    private final RootProvider rootProvider;
    private final TreeProvider treeProvider;
    private final String authorizableRootPath;
    private final Context aggregatedCtx;
    private final IdentityProtectionType protectionType;
    
    private Root rootBefore;
    private Root rootAfter;
    
    ExternalUserValidatorProvider(@NotNull RootProvider rootProvider, 
                                  @NotNull TreeProvider treeProvider, 
                                  @NotNull SecurityProvider securityProvider,
                                  @NotNull IdentityProtectionType protectionType) {
        checkArgument(protectionType != IdentityProtectionType.NONE);
        this.rootProvider = rootProvider;
        this.treeProvider = treeProvider;
        this.protectionType = protectionType;

        this.authorizableRootPath = UserUtil.getAuthorizableRootPath(securityProvider.getParameters(UserConfiguration.NAME), AuthorizableType.AUTHORIZABLE);
        aggregatedCtx = new AggregatedContext(securityProvider);
        
    }

    @Override
    protected @NotNull Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        this.rootBefore = rootProvider.createReadOnlyRoot(before);
        this.rootAfter = rootProvider.createReadOnlyRoot(after);
        return new SubtreeValidator(new ExternalUserValidator(), Iterables.toArray(PathUtils.elements(authorizableRootPath), String.class));
    }
    
    private class ExternalUserValidator extends DefaultValidator {
        
        private Tree parentBefore;
        private Tree parentAfter;
        
        boolean isExternalIdentity = false;
        
        private ExternalUserValidator() {}
        
        private ExternalUserValidator(@NotNull ExternalUserValidator parentValidator, @NotNull Tree parentBefore, @NotNull Tree parentAfter) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;

            setExternalIdentity(parentValidator, parentBefore);
        }

        private ExternalUserValidator(@NotNull ExternalUserValidator parentValidator, @NotNull Tree parent, boolean isBefore) {
            if (isBefore) {
                this.parentBefore = parent;
                setExternalIdentity(parentValidator, parentBefore);
            } else {
                this.parentAfter = parent;
                setExternalIdentity(parentValidator, parentAfter);
            }
        }
        
        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            Tree afterTree = getParentAfter();
            if (definedSecurityContext(afterTree, after)) {
                return;
            }

            if (isModifyingExternalIdentity(isExternalIdentity, after)) {
                String msg = String.format("Attempt to add property '%s' to protected external identity node '%s'", after.getName(), afterTree.getPath());
                handleViolation(msg);
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            Tree beforeTree = getParentBefore();
            if (definedSecurityContext(beforeTree, before)) {
                return;
            }
            if (isModifyingExternalIdentity(isExternalIdentity, before)) {
                String msg = String.format("Attempt to modify property '%s' at protected external identity node '%s'", before.getName(), beforeTree.getPath());
                handleViolation(msg);
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            Tree beforeTree = getParentBefore();
            if (definedSecurityContext(beforeTree, before)) {
                return;
            }
            if (isModifyingExternalIdentity(isExternalIdentity, before)) {
                String msg = String.format("Attempt to delete property '%s' from protected external identity node '%s'", before.getName(), beforeTree.getPath());
                handleViolation(msg);
            }
        }

        @Override
        public @Nullable Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            Tree afterParent = getParentAfter();
            Tree afterTree = treeProvider.createReadOnlyTree(afterParent, name, after);
            if (definedSecurityContext(afterTree, null)) {
                return null;
            }

            if (isExternalIdentity(afterTree)) {
                String msg = String.format("Attempt to add protected external identity '%s'", afterTree.getPath());
                handleViolation(msg);
                return null;
            } else if (isModifyingExternalIdentity(isExternalIdentity, null)) {
                String msg = String.format("Attempt to add node '%s' to protected external identity node '%s'", name, afterParent.getPath());
                handleViolation(msg);
                return null;
            } else if (UserUtil.isType(afterTree, AuthorizableType.AUTHORIZABLE)) {
                // user/group creation (not protected) -> no traversal into the subtree needed
                return null;
            } else {
                return new ExternalUserValidator(this, afterTree, false);
            }
        }

        @Override
        public @Nullable Validator childNodeChanged(String name, NodeState before, NodeState after) {
            Tree beforeTree = treeProvider.createReadOnlyTree(getParentBefore(), name, before);
            Tree afterTree = treeProvider.createReadOnlyTree(getParentAfter(), name, after);
            
            if (definedSecurityContext(beforeTree, null)) {
                return null;
            }
            return new ExternalUserValidator(this, beforeTree, afterTree);
        }

        @Override
        public @Nullable Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            Tree beforeTree = treeProvider.createReadOnlyTree(getParentBefore(), name, before);
            if (definedSecurityContext(beforeTree, null)) {
                return null;
            }
            
            if (isExternalIdentity(beforeTree)) {
                // attempt to remove an external identity
                String msg = String.format("Attempt to remove protected external identity '%s'", beforeTree.getPath());
                handleViolation(msg);
                return null;
            }
            
            if (isModifyingExternalIdentity(isExternalIdentity, null)) {
                // attempt to remove a node below an external user/group
                String msg = String.format("Attempt to remove node '%s' from protected external identity", beforeTree.getPath());
                handleViolation(msg);
                return null;
            }
            
            // decend into subtree to spot any removal of external user/group or it's subtree
            return new ExternalUserValidator(this, beforeTree, true);
        }
        
        private void setExternalIdentity(@NotNull ExternalUserValidator parentValidator, @NotNull Tree parent) {
            if (parentValidator.isExternalIdentity) {
                this.isExternalIdentity = true;
            } else {
                this.isExternalIdentity = isExternalIdentity(parent);
            }
        }
        
        private boolean isExternalIdentity(@NotNull Tree tree) {
            return UserUtil.isType(tree, AuthorizableType.AUTHORIZABLE) && tree.hasProperty(REP_EXTERNAL_ID);
        }
        
        private @NotNull Tree getParentBefore() {
            if (parentBefore == null) {
                parentBefore = rootBefore.getTree(authorizableRootPath);
            }
            return parentBefore;
        }
        
        private @NotNull Tree getParentAfter() {
            if (parentAfter == null) {
                parentAfter = rootAfter.getTree(authorizableRootPath);
            }
            return parentAfter;
        }
        
        private boolean isModifyingExternalIdentity(boolean insideAuthorizable, @Nullable PropertyState propertyState) {
            return insideAuthorizable && !isExcludedProperty(propertyState);
        }

        /**
         * Adding mixin types that define security-related content as this is not a 
         * modification of the user/group that is exposed through user-mgt API. Note however, that editing non-security
         * related mixins would still fail as the child items defined by the mixin type cannot be written.
         * 
         * @param propertyState The property to be tested
         * @return {@code true} if the given property is excluded from protection
         */
        private boolean isExcludedProperty(@Nullable PropertyState propertyState) {
            if (propertyState == null) {
                return false;
            } else {
                return JCR_MIXINTYPES.equals(propertyState.getName());
            }
        }
        
        private boolean definedSecurityContext(@NotNull Tree tree, @Nullable PropertyState propertyState) {
            if (propertyState != null) {
                return aggregatedCtx.definesProperty(tree, propertyState);
            } else {
                return aggregatedCtx.definesTree(tree);
            }
        }
        
        private void handleViolation(@NotNull String msg) throws CommitFailedException {
            if (protectionType == IdentityProtectionType.WARN) {
                log.warn(msg);
            } else {
                // the validator is never create with IdentityProtectionType.NONE
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 76, msg);
            }
        }
    }
    
    private static final class AggregatedContext extends Context.Default {
        
        List<Context> ctxs;
        
        private AggregatedContext(@NotNull SecurityProvider securityProvider) {
            ImmutableList.Builder<Context> builder = ImmutableList.builder();
            for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
                if (!UserConfiguration.NAME.equals(sc.getName())) {
                    builder.add(sc.getContext());
                }
            }
            ctxs = builder.build();
        }

        @Override
        public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
            return ctxs.stream().anyMatch(context -> context.definesProperty(parent, property));
        }

        @Override
        public boolean definesTree(@NotNull Tree tree) {
            return ctxs.stream().anyMatch(context -> context.definesTree(tree));
        }
    }
}