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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * UserValidator... TODO
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
        String name = after.getName();
        if (REP_DISABLED.equals(name) && isAdminUser()) {
            throw new CommitFailedException("Admin user cannot be disabled.");
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        String name = before.getName();
        if (REP_PRINCIPAL_NAME.equals(name) || REP_AUTHORIZABLE_ID.equals(name)) {
            throw new CommitFailedException("Authorizable property " + name + " may not be altered after user/group creation.");
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        NodeUtil node = parentAfter.getChild(name);
        String authRoot = null;
        if (node.hasPrimaryNodeTypeName(NT_REP_USER)) {
            authRoot = provider.getConfig().getConfigValue(UserConfig.PARAM_USER_PATH, DEFAULT_USER_PATH);
        } else if (node.hasPrimaryNodeTypeName(UserConstants.NT_REP_GROUP)) {
            authRoot = provider.getConfig().getConfigValue(UserConfig.PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        }
        if (authRoot != null) {
            assertHierarchy(node, authRoot);
        }
        return new UserValidator(null, node, provider);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // TODO: anything to do here?
        return new UserValidator(parentBefore.getChild(name), parentAfter.getChild(name), provider);
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
            Exception e = new ConstraintViolationException("Attempt to create user/group outside of configured scope " + pathConstraint);
            throw new CommitFailedException(e);
        }

        NodeUtil parent = userNode.getParent();
        while (!parent.getTree().isRoot()) {
            if (!parent.hasPrimaryNodeTypeName(NT_REP_AUTHORIZABLE_FOLDER)) {
                String msg = "Cannot create user/group: Intermediate folders must be of type rep:AuthorizableFolder.";
                Exception e = new ConstraintViolationException(msg);
                throw new CommitFailedException(e);
            }
            parent = parent.getParent();
        }
    }

    private boolean isAdminUser() {
        // FIXME: add implementation
        return false;
    }
}