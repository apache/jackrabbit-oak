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
package org.apache.jackrabbit.oak.security.authentication.token;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

class TokenValidatorProvider extends ValidatorProvider implements TokenConstants {

    private static final Logger log = LoggerFactory.getLogger(TokenValidatorProvider.class);

    private final String userRootPath;

    TokenValidatorProvider(@Nonnull ConfigurationParameters userConfig) {
        userRootPath = userConfig.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
    }

    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo commitInfo) {
        return new TokenValidator(before, after, commitInfo);
    }

    private static CommitFailedException constraintViolation(int code, @Nonnull String message) {
        return new CommitFailedException(CommitFailedException.CONSTRAINT, code, message);
    }

    private final class TokenValidator extends DefaultValidator implements TokenConstants {

        private final Tree parentBefore;
        private final Tree parentAfter;
        private final CommitInfo commitInfo;

        TokenValidator(@Nonnull NodeState parentBefore, @Nonnull NodeState parentAfter, @Nonnull CommitInfo commitInfo) {
            this(TreeFactory.createReadOnlyTree(parentBefore), TreeFactory.createReadOnlyTree(parentAfter), commitInfo);
        }

        private TokenValidator(@Nullable Tree parentBefore, @Nonnull Tree parentAfter, @Nonnull CommitInfo commitInfo) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
            this.commitInfo = commitInfo;
        }

        //------------------------------------------------------< Validator >---

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            String name = after.getName();
            if (TOKEN_PROPERTY_NAMES.contains(name)) {
                // ensure that token specific properties are managed by the token provider.
                verifyCommitInfo();
                // make sure they are not solely located with a token node.
                if (!isTokenTree(parentAfter)) {
                    String msg = "Attempt to create reserved token property " + name;
                    throw constraintViolation(60, msg);
                }
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            String propertyName = after.getName();
            if (TOKEN_ATTRIBUTE_KEY.equals(propertyName)) {
                String msg = "Attempt to change reserved token property " + propertyName;
                throw constraintViolation(61, msg);
            } else if (TOKEN_ATTRIBUTE_EXPIRY.equals(propertyName)) {
                verifyCommitInfo();
            } else if (JcrConstants.JCR_PRIMARYTYPE.equals(propertyName)) {
                if (TOKEN_NT_NAME.equals(after.getValue(Type.STRING))) {
                    throw constraintViolation(62, "Changing primary type of existing node to the reserved token node type.");
                }
                if (isTokensParent(parentAfter) && TOKENS_NT_NAME.equals(before.getValue(Type.STRING))) {
                    throw constraintViolation(69, "Cannot change the primary type of an existing .tokens node.");
                }
            }
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            Tree tree = checkNotNull(parentAfter.getChild(name));

            if (isTokenTree(tree)) {
                validateTokenTree(tree);
                // no further validation required
                return null;
            } else if (isTokensParent(tree)) {
                validateTokensParent(tree);
            }
            return new VisibleValidator(new TokenValidator(null, tree, commitInfo), true, true);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            Tree beforeTree = (parentBefore == null) ? null : parentBefore.getChild(name);
            Tree afterTree = parentAfter.getChild(name);

            if (isTokenTree(beforeTree) || isTokenTree(afterTree)) {
                validateTokenTree(afterTree);
            } else if (isTokensParent(beforeTree) || isTokensParent(afterTree)) {
                validateTokensParent(afterTree);
            }

            return new VisibleValidator(new TokenValidator(beforeTree, afterTree, commitInfo), true, true);
        }

        //--------------------------------------------------------< private >---
        private void verifyCommitInfo() throws CommitFailedException {
            if (!CommitMarker.isValidCommitInfo(commitInfo)) {
                throw constraintViolation(63, "Attempt to manually create or change a token node or it's parent.");
            }
        }

        private void verifyHierarchy(@Nonnull String path) throws CommitFailedException {
            if (!Text.isDescendant(userRootPath, path)) {
                String msg = "Attempt to create a token (or it's parent) outside of configured scope " + path;
                throw constraintViolation(64, msg);
            }
        }

        private boolean isTokenTree(@CheckForNull Tree tree) {
            return tree != null && TOKEN_NT_NAME.equals(TreeUtil.getPrimaryTypeName(tree));
        }

        private void validateTokenTree(@Nonnull Tree tokenTree) throws CommitFailedException {
            // enforce changing being made by the TokenProvider implementation
            verifyCommitInfo();

            verifyHierarchy(tokenTree.getPath());

            Tree parent = tokenTree.getParent();
            if (!isTokensParent(parent) || !UserConstants.NT_REP_USER.equals(TreeUtil.getPrimaryTypeName(parent.getParent()))) {
                throw constraintViolation(65, "Invalid location of token node.");
            }

            // assert mandatory properties are present
            String key = TreeUtil.getString(tokenTree, TOKEN_ATTRIBUTE_KEY);
            if (PasswordUtil.isPlainTextPassword(key)) {
                throw constraintViolation(66, "Invalid token key.");
            }

            if (TreeUtil.getString(tokenTree, TOKEN_ATTRIBUTE_EXPIRY) == null) {
                throw constraintViolation(67, "Mandatory token expiration missing.");
            }
        }

        private boolean isTokensParent(@CheckForNull Tree tree) {
            return tree != null && TOKENS_NODE_NAME.equals(tree.getName());
        }

        private void validateTokensParent(@Nonnull Tree tokensParent) throws CommitFailedException {

            verifyHierarchy(tokensParent.getPath());

            Tree userTree = tokensParent.getParent();
            if (!UserConstants.NT_REP_USER.equals(TreeUtil.getPrimaryTypeName(userTree))) {
                throw constraintViolation(68, "Invalid location of .tokens node.");
            }

            String nt = TreeUtil.getPrimaryTypeName(tokensParent);
            if (!TOKENS_NT_NAME.equals(nt)) {
                log.debug("Unexpected node type of .tokens node " + nt + '.');
            }
        }
    }
}