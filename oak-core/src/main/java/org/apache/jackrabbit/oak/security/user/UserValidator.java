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

import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * Validator that enforces user management specific constraints. Please note that
 * is this validator is making implementation specific assumptions; if the
 * user management implementation is replace it is most probably necessary to
 * provide a custom validator as well.
 */
class UserValidator extends DefaultValidator implements UserConstants {

    private final UserValidatorProvider provider;

    private final NodeUtil parentBefore;
    private final NodeUtil parentAfter;

    UserValidator(NodeUtil parentBefore, NodeUtil parentAfter, UserValidatorProvider provider) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;

        this.provider = provider;
    }

    //----------------------------------------------------------< Validator >---

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (!isAuthorizable(parentAfter)) {
            return;
        }

        String name = after.getName();
        if (REP_DISABLED.equals(name) && isAdminUser(parentAfter)) {
            String msg = "Admin user cannot be disabled.";
            fail(msg);
        }

        if (JcrConstants.JCR_UUID.equals(name) && !isValidUUID(after.getValue(Type.STRING))) {
            String msg = "Invalid jcr:uuid for authorizable " + parentAfter.getName();
            fail(msg);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (!isAuthorizable(parentAfter)) {
            return;
        }

        String name = before.getName();
        if (REP_PRINCIPAL_NAME.equals(name) || REP_AUTHORIZABLE_ID.equals(name)) {
            String msg = "Authorizable property " + name + " may not be altered after user/group creation.";
            fail(msg);
        } else if (JcrConstants.JCR_UUID.equals(name) && !isValidUUID(after.getValue(Type.STRING))) {
            String msg = "Invalid jcr:uuid for authorizable " + parentAfter.getName();
            fail(msg);
        }

        if (isUser(parentBefore) && REP_PASSWORD.equals(name) && PasswordUtility.isPlainTextPassword(after.getValue(Type.STRING))) {
            String msg = "Password may not be plain text.";
            fail(msg);
        }
    }


    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (!isAuthorizable(parentAfter)) {
            return;
        }

        String name = before.getName();
        if (REP_PASSWORD.equals(name) || REP_PRINCIPAL_NAME.equals(name) || REP_AUTHORIZABLE_ID.equals(name)) {
            String msg = "Authorizable property " + name + " may not be removed.";
            fail(msg);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        NodeUtil node = parentAfter.getChild(name);
        String authRoot = null;
        if (node.hasPrimaryNodeTypeName(NT_REP_USER)) {
            authRoot = provider.getConfig().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH);
        } else if (node.hasPrimaryNodeTypeName(UserConstants.NT_REP_GROUP)) {
            authRoot = provider.getConfig().getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        }
        if (authRoot != null) {
            assertHierarchy(node, authRoot);
            // assert rep:principalName is present (that should actually by covered
            // by node type validator)
            if (node.getString(REP_PRINCIPAL_NAME, null) == null) {
                fail("Mandatory property rep:principalName missing.");
            }
        }
        return new UserValidator(null, node, provider);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // TODO: anything to do here?
        return new UserValidator(parentBefore.getChild(name), parentAfter.getChild(name), provider);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        NodeUtil node = parentBefore.getChild(name);
        if (isAdminUser(node)) {
            String msg = "The admin user cannot be removed.";
            fail(msg);
        }
        return null;
    }

    //------------------------------------------------------------< private >---

    /**
     * Make sure user and group nodes are located underneath the configured path
     * and that path consists of rep:authorizableFolder nodes.
     *
     * @param userNode
     * @param pathConstraint
     * @throws CommitFailedException
     */
    private void assertHierarchy(NodeUtil userNode, String pathConstraint) throws CommitFailedException {
        if (!Text.isDescendant(pathConstraint, userNode.getTree().getPath())) {
            String msg = "Attempt to create user/group outside of configured scope " + pathConstraint;
            fail(msg);
        }

        NodeUtil parent = userNode.getParent();
        while (!parent.getTree().isRoot()) {
            if (!parent.hasPrimaryNodeTypeName(NT_REP_AUTHORIZABLE_FOLDER)) {
                String msg = "Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.";
                fail(msg);
            }
            parent = parent.getParent();
        }
    }


    // FIXME: copied from UserProvider#isAdminUser
    private boolean isAdminUser(NodeUtil userNode) {
        String id = (userNode.getString(REP_AUTHORIZABLE_ID, Text.unescapeIllegalJcrChars(userNode.getName())));
        return isUser(userNode) && UserUtility.getAdminId(provider.getConfig()).equals(id);
    }

    private boolean isValidUUID(String uuid) {
        String id = UserProvider.getAuthorizableId(parentAfter.getTree());
        return uuid.equals(UserProvider.getContentID(id));
    }

    private static boolean isAuthorizable(NodeUtil node) {
        return UserUtility.isType(node.getTree(), AuthorizableType.AUTHORIZABLE);
    }

    private static boolean isUser(NodeUtil node) {
        return UserUtility.isType(node.getTree(), AuthorizableType.USER);
    }



    private static void fail(String msg) throws CommitFailedException {
        Exception e = new ConstraintViolationException(msg);
        throw new CommitFailedException(e);
    }
}