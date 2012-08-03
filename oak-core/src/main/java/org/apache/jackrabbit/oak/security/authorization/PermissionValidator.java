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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;

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
        int permissions = getPermissions(after, Permissions.ADD_PROPERTY);
        checkPermissions(getPath(after), permissions);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        int permissions = getPermissions(after, Permissions.MODIFY_PROPERTY);
        checkPermissions(getPath(after), permissions);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        int permissions = getPermissions(before, Permissions.REMOVE_PROPERTY);
        checkPermissions(getPath(before), permissions);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        ReadOnlyTree child = new ReadOnlyTree(parentAfter, name, after);

        int permissions = getPermissions(child, Permissions.ADD_NODE);
        checkPermissions(child.getPath(), permissions);

        return new PermissionValidator(compiledPermissions, null, child);
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

        int permissions = getPermissions(child, Permissions.REMOVE_NODE);
        checkPermissions(child.getPath(), permissions);

        return new PermissionValidator(compiledPermissions, child, null);
    }

    //------------------------------------------------------------< private >---
    private void checkPermissions(String path, int permissions) throws CommitFailedException {
        if (!compiledPermissions.isGranted(path, permissions))    {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }


    private int getPermissions(PropertyState property, int defaultPermission) {
        if (isProtected(property)) {
            // TODO: identify specific permission depending on type of protection
            // - access controlled property
            // - lock property
            // - version property
            // - mixinType/primaryType -> nt-management permission
            // - node type definition -> nt-definition-management
            // - namespace definition -> namespace management
            // - privilege -> privilege management
            // - user/group property -> user management
            return Permissions.NO_PERMISSION;
        } else if (PropertyState.OAK_CHILD_ORDER.equals(property.getName())) {
            return Permissions.MODIFY_CHILD_NODE_COLLECTION;
        } else {
            return defaultPermission;
        }
    }

    private int getPermissions(ReadOnlyTree tree, int defaultPermissions) {
        if (isProtected(tree)) {
            // TODO: identify specific permission depending on type of protection
            return Permissions.NO_PERMISSION;
        } else {
            return defaultPermissions;
        }
    }

    private boolean isProtected(PropertyState property) {
        // TODO
        return false;
    }

    private boolean isProtected(ReadOnlyTree tree) {
        // TODO
        return false;
    }

    private String getPath(PropertyState property) {
        String parentPath = (parentAfter != null) ? parentAfter.getPath() : parentBefore.getPath();
        return PathUtils.concat(parentPath, property.getName());
    }
}