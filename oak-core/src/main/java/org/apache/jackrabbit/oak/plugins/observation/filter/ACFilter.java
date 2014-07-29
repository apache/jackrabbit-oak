/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code EventTypeFilter} filters based on the access rights of the observing session.
 */
public class ACFilter implements EventFilter {
    private final TreePermission treePermission;

    private ACFilter(@Nonnull TreePermission treePermission) {
        this.treePermission = checkNotNull(treePermission);
    }

    /**
     * Create a new {@code Filter} instance that includes an event when the
     * observing session has sufficient permissions to read the associated item.
     *
     * @param before  before state
     * @param after  after state
     * @param permissionProvider  permission provider for access control evaluation
     */
    public ACFilter(@Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull PermissionProvider permissionProvider) {
        this(permissionProvider.getTreePermission(
                new ImmutableTree(after.exists() ? after : before), TreePermission.EMPTY));
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return treePermission.canRead(after);
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return treePermission.canRead(after);
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return treePermission.canRead(before);
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return treePermission.getChildPermission(name, after).canRead();
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return treePermission.getChildPermission(name, before).canRead();
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        // TODO: check access to the source path, it might not be accessible
        return treePermission.getChildPermission(name, moved).canRead();
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        // TODO: check access to the dest name, it might not be accessible
        return treePermission.getChildPermission(name, reordered).canRead();
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        return new ACFilter(treePermission.getChildPermission(name, after));
    }

}
