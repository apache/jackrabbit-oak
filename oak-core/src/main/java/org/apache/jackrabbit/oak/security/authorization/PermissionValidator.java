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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

/**
 * PermissionValidator... TODO
 */
public class PermissionValidator implements Validator {

    /* TODO
     * - special permissions for protected items (versioning, access control, etc.)
     * - Renaming nodes or Move with same parent are reflected as remove+add -> needs special handling
     * - review usage of OAK_CHILD_ORDER property (in particular if the property was removed
     *
     */

    private final CompiledPermissions compiledPermissions;

    private final ReadOnlyTree parentBefore;
    private final ReadOnlyTree parentAfter;

    PermissionValidator(CompiledPermissions compiledPermissions,
                        ReadOnlyTree parentBefore, ReadOnlyTree parentAfter) {
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
        ReadOnlyTree child = new ReadOnlyTree(parentAfter, name, after);
        return checkPermissions(child, false, Permissions.ADD_NODE);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        ReadOnlyTree childBefore = new ReadOnlyTree(parentBefore, name, before);
        ReadOnlyTree childAfter = new ReadOnlyTree(parentAfter, name, after);

        // TODO

        return new PermissionValidator(compiledPermissions, childBefore, childAfter);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        ReadOnlyTree child = new ReadOnlyTree(parentBefore, name, before);
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //------------------------------------------------------------< private >---
    private void checkPermissions(Tree parent, PropertyState property, int defaultPermission) throws CommitFailedException {
        String parentPath = parent.getPath();
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
        } else if (isAccessControl(parent, property)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else {
            // TODO: identify specific permission depending on type of protection
            // - version property -> version management
            // - user/group property -> user management
            permission = defaultPermission;
        }

        checkPermissions(PathUtils.concat(parentPath, name), permission);
    }

    private PermissionValidator checkPermissions(ReadOnlyTree tree, boolean isBefore, int defaultPermission) throws CommitFailedException {
        String path = tree.getPath();
        int permission;

        if (isNamespaceDefinition(path)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (isNodeTypeDefinition(path)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (isPrivilegeDefinition(path)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else if (isAccessControl(tree)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else {
            // TODO: identify specific permission depending on additional types of protection
            // - versioning -> version management
            // - user/group -> user management
            // - workspace management ???
            permission = defaultPermission;
        }

        if (Permissions.isRepositoryPermissions(permission)) {
            checkPermissions((String) null, permission);
            return null; // no need for further validation down the subtree
        } else {
            checkPermissions(path, permission);
            return (isBefore) ?
                    new PermissionValidator(compiledPermissions, tree, null) :
                    new PermissionValidator(compiledPermissions, null, tree);
        }
    }

    private void checkPermissions(String path, int permissions) throws CommitFailedException {
        if (!compiledPermissions.isGranted(path, permissions))    {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }

    private static boolean isAccessControl(Tree parent) {
        // TODO: depends on ac-model
        return false;
    }

    private static boolean isAccessControl(Tree parent, PropertyState property) {
        // TODO: depends on ac-model
        return false;
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