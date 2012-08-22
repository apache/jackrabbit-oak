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

import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.version.VersionConstants;
import org.apache.jackrabbit.util.Text;

/**
 * PermissionValidator... TODO
 */
class PermissionValidator implements Validator {

    /* TODO
     * - special permissions for protected items (versioning, access control, etc.)
     * - Renaming nodes or Move with same parent are reflected as remove+add -> needs special handling
     * - review usage of OAK_CHILD_ORDER property (in particular if the property was removed
     */

    private final CompiledPermissions compiledPermissions;

    private final NodeUtil parentBefore;
    private final NodeUtil parentAfter;

    PermissionValidator(CompiledPermissions compiledPermissions,
                        NodeUtil parentBefore, NodeUtil parentAfter) {
        this.compiledPermissions = compiledPermissions;
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;

    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        checkPermissions(parentAfter, after, Permissions.ADD_PROPERTY);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        checkPermissions(parentAfter, after, Permissions.MODIFY_PROPERTY);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        checkPermissions(parentBefore, before, Permissions.REMOVE_PROPERTY);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        NodeUtil child = parentAfter.getChild(name);
        return checkPermissions(child, false, Permissions.ADD_NODE);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        NodeUtil childBefore = parentBefore.getChild(name);
        NodeUtil childAfter = parentAfter.getChild(name);

        // TODO

        return new PermissionValidator(compiledPermissions, childBefore, childAfter);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        NodeUtil child = parentBefore.getChild(name);
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //------------------------------------------------------------< private >---
    private void checkPermissions(NodeUtil parent, PropertyState property, int defaultPermission) throws CommitFailedException {
        String parentPath = parent.getTree().getPath();
        String name = property.getName();

        int permission;
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name) || JcrConstants.JCR_MIXINTYPES.equals(name)) {
            // TODO: distinguish between autocreated and user-supplied modification (?)
            permission = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (PropertyState.OAK_CHILD_ORDER.equals(property.getName())) {
            permission = Permissions.MODIFY_CHILD_NODE_COLLECTION;
        } else if (isLockProperty(name)) {
            permission = Permissions.LOCK_MANAGEMENT;
        } else if (isNamespaceDefinition(parentPath)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (isNodeTypeDefinition(parentPath)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (isPrivilegeDefinition(parentPath)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else if (isAccessControl(parent)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (isVersionProperty(parent, property)) {
            permission = Permissions.VERSION_MANAGEMENT;
            // FIXME: path to check for permission must be adjusted to be
            //        the one of the versionable node instead of the target parent.
        } else {
            // TODO: identify specific permission depending on type of protection
            // - user/group property -> user management
            permission = defaultPermission;
        }

        checkPermissions(parent.getTree(), property, permission);
    }

    private PermissionValidator checkPermissions(NodeUtil node, boolean isBefore, int defaultPermission) throws CommitFailedException {
        String path = node.getTree().getPath();
        int permission;

        if (isNamespaceDefinition(path)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (isNodeTypeDefinition(path)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (isPrivilegeDefinition(path)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else if (isAccessControl(node)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (isVersion(node)) {
            permission = Permissions.VERSION_MANAGEMENT;
            // FIXME: path to check for permission must be adjusted to be
            // //     the one of the versionable node instead of the target node.
        } else {
            // TODO: identify specific permission depending on additional types of protection
            // - user/group -> user management
            // - workspace management ???
            // TODO: identify renaming/move of nodes that only required MODIFY_CHILD_NODE_COLLECTION permission
            permission = defaultPermission;
        }

        if (Permissions.isRepositoryPermission(permission)) {
            checkPermissions(permission);
            return null; // no need for further validation down the subtree
        } else {
            checkPermissions(node.getTree(), permission);
            return (isBefore) ?
                    new PermissionValidator(compiledPermissions, node, null) :
                    new PermissionValidator(compiledPermissions, null, node);
        }
    }

    private void checkPermissions(int permissions) throws CommitFailedException {
        if (!compiledPermissions.isGranted(permissions))    {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }

    private void checkPermissions(Tree tree, int permissions) throws CommitFailedException {
        if (!compiledPermissions.isGranted(tree, permissions))    {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }

    private void checkPermissions(Tree parent, PropertyState property, int permissions) throws CommitFailedException {
        if (!compiledPermissions.isGranted(parent, property, permissions))    {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }

    private static boolean isAccessControl(NodeUtil node) {
        // TODO: depends on ac-model
        return false;
    }

    private static boolean isVersion(NodeUtil node) {
        if (node.getTree().isRoot()) {
            return false;
        }
        // TODO: review again
        if (VersionConstants.VERSION_NODE_NAMES.contains(node.getName())) {
            return true;
        } else if (VersionConstants.VERSION_NODE_TYPE_NAMES.contains(node.getName(JcrConstants.JCR_PRIMARYTYPE))) {
            return true;
        } else {
            String path = node.getTree().getPath();
            return VersionConstants.SYSTEM_PATHS.contains(Text.getAbsoluteParent(path, 1));
        }
    }

    private static boolean isVersionProperty(NodeUtil parent, PropertyState property) {
        if (VersionConstants.VERSION_PROPERTY_NAMES.contains(property.getName())) {
            return true;
        } else {
            return isVersion(parent);
        }
    }

    private static boolean isLockProperty(String name) {
        return JcrConstants.JCR_LOCKISDEEP.equals(name) || JcrConstants.JCR_LOCKOWNER.equals(name);
    }

    private static boolean isNamespaceDefinition(String path) {
        return Text.isDescendant(NamespaceConstants.NAMESPACES_PATH, path);
    }
    private static boolean isNodeTypeDefinition(String path) {
        return Text.isDescendant(NodeTypeConstants.NODE_TYPES_PATH, path);
    }

    private static boolean isPrivilegeDefinition(String path) {
        return Text.isDescendant(PrivilegeConstants.PRIVILEGES_PATH, path);
    }
}