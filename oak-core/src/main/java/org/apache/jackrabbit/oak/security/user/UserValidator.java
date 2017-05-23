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
package org.apache.jackrabbit.oak.security.user;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Validator that enforces user management specific constraints. Please note that
 * is this validator is making implementation specific assumptions; if the
 * user management implementation is replace it is most probably necessary to
 * provide a custom validator as well.
 */
class UserValidator extends DefaultValidator implements UserConstants {

    private final Tree parentBefore;
    private final Tree parentAfter;
    private final UserValidatorProvider provider;

    private final AuthorizableType authorizableType;

    UserValidator(Tree parentBefore, Tree parentAfter, UserValidatorProvider provider) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.provider = provider;

        authorizableType = (parentAfter == null) ? null : UserUtil.getType(parentAfter);
    }

    //----------------------------------------------------------< Validator >---

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (authorizableType == null) {
            return;
        }

        String name = after.getName();
        if (REP_DISABLED.equals(name) && isAdminUser(parentAfter)) {
            String msg = "Admin user cannot be disabled.";
            throw constraintViolation(20, msg);
        }

        if (JcrConstants.JCR_UUID.equals(name) && !isValidUUID(parentAfter, after.getValue(Type.STRING))) {
            String msg = "Invalid jcr:uuid for authorizable " + parentAfter.getName();
            throw constraintViolation(21, msg);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (authorizableType == null) {
            return;
        }

        String name = before.getName();
        if (REP_PRINCIPAL_NAME.equals(name) || REP_AUTHORIZABLE_ID.equals(name)) {
            String msg = "Authorizable property " + name + " may not be altered after user/group creation.";
            throw constraintViolation(22, msg);
        } else if (JcrConstants.JCR_UUID.equals(name)) {
            checkNotNull(parentAfter);
            if (!isValidUUID(parentAfter, after.getValue(Type.STRING))) {
                String msg = "Invalid jcr:uuid for authorizable " + parentAfter.getName();
                throw constraintViolation(23, msg);
            }
        } else if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            // if primary type changed to authorizable type -> need to perform
            // validation as if a new authorizable node had been added
            validateAuthorizable(parentAfter, UserUtil.getType(after.getValue(Type.STRING)));
        }

        if (isUser(parentBefore) && REP_PASSWORD.equals(name) && PasswordUtil.isPlainTextPassword(after.getValue(Type.STRING))) {
            String msg = "Password may not be plain text.";
            throw constraintViolation(24, msg);
        }
    }


    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (authorizableType == null) {
            return;
        }

        String name = before.getName();
        if (REP_PASSWORD.equals(name) || REP_PRINCIPAL_NAME.equals(name) || REP_AUTHORIZABLE_ID.equals(name)) {
            String msg = "Authorizable property " + name + " may not be removed.";
            throw constraintViolation(25, msg);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        Tree tree = checkNotNull(parentAfter.getChild(name));

        validateAuthorizable(tree, UserUtil.getType(tree));
        return newValidator(null, tree, provider);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return newValidator(parentBefore.getChild(name),
                parentAfter.getChild(name), provider);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Tree tree = parentBefore.getChild(name);
        AuthorizableType type = UserUtil.getType(tree);
        if (type == AuthorizableType.USER || type == AuthorizableType.GROUP) {
            if (isAdminUser(tree)) {
                String msg = "The admin user cannot be removed.";
                throw constraintViolation(27, msg);
            }
            return null;
        } else {
            return newValidator(tree, null, provider);
        }
    }

    //------------------------------------------------------------< private >---

    private static Validator newValidator(Tree parentBefore,
                                          Tree parentAfter,
                                          UserValidatorProvider provider) {
        return new VisibleValidator(
                new UserValidator(parentBefore, parentAfter, provider),
                true,
                true);
    }

    private boolean isAdminUser(@Nonnull Tree userTree) {
        if (userTree.exists() && isUser(userTree)) {
            String id = UserUtil.getAuthorizableId(userTree);
            return UserUtil.getAdminId(provider.getConfig()).equals(id);
        } else {
            return false;
        }
    }

    private void validateAuthorizable(@Nonnull Tree tree, @Nullable AuthorizableType type) throws CommitFailedException {
        boolean isSystemUser = (type == AuthorizableType.USER) && UserUtil.isSystemUser(tree);
        String authRoot = UserUtil.getAuthorizableRootPath(provider.getConfig(), type);
        if (isSystemUser) {
            String sysRelPath = provider.getConfig().getConfigValue(PARAM_SYSTEM_RELATIVE_PATH, DEFAULT_SYSTEM_RELATIVE_PATH);
            authRoot = authRoot + '/' + sysRelPath;
        }
        if (authRoot != null) {
            assertHierarchy(tree, authRoot);
            // assert rep:principalName is present (that should actually by covered
            // by node type validator)
            if (TreeUtil.getString(tree, REP_PRINCIPAL_NAME) == null) {
                throw constraintViolation(26, "Mandatory property rep:principalName missing.");
            }

            if (isSystemUser) {
                if (TreeUtil.getString(tree, REP_PASSWORD) != null) {
                    throw constraintViolation(32, "Attempt to set password with system user.");
                }
                if (tree.hasChild(REP_PWD)) {
                    throw constraintViolation(33, "Attempt to add rep:pwd node to a system user.");
                }
            }
        }
    }

    private boolean isValidUUID(@Nonnull Tree parent, @Nonnull String uuid) {
        String id = UserUtil.getAuthorizableId(parent);
        return id != null && uuid.equals(provider.getMembershipProvider().getContentID(id));
    }

    private static boolean isUser(@Nullable Tree tree) {
        return tree != null && NT_REP_USER.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    /**
     * Make sure user and group nodes are located underneath the configured path
     * and that path consists of rep:authorizableFolder nodes.
     *
     * @param tree           The tree representing a user or group.
     * @param pathConstraint The path constraint.
     * @throws CommitFailedException If the hierarchy isn't valid.
     */
    private static void assertHierarchy(@Nonnull Tree tree, @Nonnull String pathConstraint) throws CommitFailedException {
        if (!Text.isDescendant(pathConstraint, tree.getPath())) {
            String msg = "Attempt to create user/group outside of configured scope " + pathConstraint;
            throw constraintViolation(28, msg);
        }
        if (!tree.isRoot()) {
            Tree parent = tree.getParent();
            while (parent.exists() && !parent.isRoot()) {
                if (!NT_REP_AUTHORIZABLE_FOLDER.equals(TreeUtil.getPrimaryTypeName(parent))) {
                    String msg = "Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.";
                    throw constraintViolation(29, msg);
                }
                parent = parent.getParent();
            }
        }
    }

    private static CommitFailedException constraintViolation(int code, @Nonnull String message) {
        return new CommitFailedException(CommitFailedException.CONSTRAINT, code, message);
    }
}