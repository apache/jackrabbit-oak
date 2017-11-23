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
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code EventTypeFilter} filters based on the access rights of the observing session.
 */
public final class ACFilter implements EventFilter {

    private final NodeState before;

    private final NodeState after;

    private final String name;

    private final ACFilter parentFilter;

    private final PermissionProvider permissionProvider;

    private TreePermission treePermission;

    private ACFilter(@Nonnull NodeState before,
                     @Nonnull NodeState after,
                     @Nonnull String name,
                     @Nonnull PermissionProvider permissionProvider,
                     @Nonnull ACFilter parentFilter) {
        this.before = checkNotNull(before);
        this.after = checkNotNull(after);
        this.name = checkNotNull(name);
        this.permissionProvider = checkNotNull(permissionProvider);
        this.parentFilter = checkNotNull(parentFilter);
    }

    /**
     * Create a new {@code Filter} instance that includes an event when the
     * observing session has sufficient permissions to read the associated item.
     *
     * @param before  before state
     * @param after  after state
     * @param permissionProvider  permission provider for access control evaluation
     */
    public ACFilter(@Nonnull NodeState before,
                    @Nonnull NodeState after,
                    @Nonnull PermissionProvider permissionProvider) {
        this.before = checkNotNull(before);
        this.after = checkNotNull(after);
        this.name = null;
        this.permissionProvider = checkNotNull(permissionProvider);
        this.parentFilter = null;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return getTreePermission().canRead(after);
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return getTreePermission().canRead(after);
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return getTreePermission().canRead(before);
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return getTreePermission().getChildPermission(name, after).canRead();
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return getTreePermission().getChildPermission(name, before).canRead();
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        // TODO: check access to the source path, it might not be accessible
        return getTreePermission().getChildPermission(name, moved).canRead();
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        // TODO: check access to the dest name, it might not be accessible
        return getTreePermission().getChildPermission(name, reordered).canRead();
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        return new ACFilter(before, after, name, permissionProvider, this);
    }

    //-----------------------------< internal >---------------------------------

    private TreePermission getTreePermission() {
        TreePermission tp = treePermission;
        if (tp == null) {
            if (parentFilter == null) {
                tp = permissionProvider.getTreePermission(
                        TreeFactory.createReadOnlyTree((after.exists() ? after : before)), TreePermission.EMPTY);
            } else {
                tp = parentFilter.getTreePermission().getChildPermission(name, after);
            }
            treePermission = tp;
        }
        return tp;
    }
}
