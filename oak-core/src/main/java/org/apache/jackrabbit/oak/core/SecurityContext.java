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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The security context encapsulates all the information needed to make
 * read permission checks within a specific subtree.
 */
class SecurityContext {

    /**
     * Underlying root state, used to optimize a common case
     * in {@link #equals(Object)}.
     */
    private final NodeState root;

    /**
     * Immutable tree based on the underlying node state.
     */
    private final ImmutableTree base;

    private final PermissionProvider permissionProvider;

    private final Context acContext;

    private TreePermission treePermission;

    SecurityContext(@Nonnull NodeState rootState,
                    @Nonnull PermissionProvider permissionProvider,
                    @Nonnull Context acContext) {
        root = rootState;
        base = new ImmutableTree(rootState, new TreeTypeProviderImpl(acContext));
        this.permissionProvider = permissionProvider;
        this.acContext = acContext;
        treePermission = permissionProvider.getTreePermission(base, TreePermission.EMPTY);
    }

    private SecurityContext(@Nonnull SecurityContext parent,
                            @Nonnull String name, @Nonnull NodeState nodeState) {
        root = parent.root;
        base = new ImmutableTree(parent.base, name, nodeState);
        permissionProvider = parent.permissionProvider;
        acContext = parent.acContext;
        treePermission = permissionProvider.getTreePermission(base, parent.treePermission);
    }

    boolean canReadThisNode() {
        return treePermission.canRead();
    }

    boolean canReadAllProperties() {
        return treePermission.canReadProperties();
    }

    boolean canReadProperty(PropertyState property) {
        return treePermission.canRead(property);
    }

    boolean canReadAll() {
        return treePermission.canReadAll();
    }

    SecurityContext getChildContext(String name, NodeState state) {
        return new SecurityContext(this, name, state);
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        } else if (object instanceof SecurityContext) {
            SecurityContext that = (SecurityContext) object;
            // TODO: We should be able to do this optimization also across
            // different revisions (root states) as long as the path,
            // the subtree, and any security-related areas like the
            // permission store are equal for both states.
            return root.equals(that.root)
                    && base.getPath().equals(that.base.getPath());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
