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
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code TreePermission} implementation for the access control policy coverying
 * repository level permissions. In this implementation these permissions are
 * managed in the policy tree defined at /rep:repoPolicy, which is considered
 * protected access control content.
 *
 * This implementation relies on the precondition that the subtree defined by the
 * /rep:repoPolicy node only consists of trees of type access control. Consequently,
 * read access to trees and properties is granted if and only if {@link Permissions#READ_ACCESS_CONTROL}
 * is granted at the repo-level.
 *
 * For the same reason any other permissions are evaluated by checking the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission},
 * which apply for all items defined by this special subtree.
 */
final class RepoPolicyTreePermission implements TreePermission {

    private RepositoryPermission repoPermission;
    private ReadStatus readStatus;

    RepoPolicyTreePermission(RepositoryPermission repoPermission) {
        this.repoPermission = repoPermission;
    }

    TreePermission getChildPermission() {
        return this;
    }

    //-----------------------------------------------------< TreePermission >---
    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        return getChildPermission();
    }

    @Override
    public boolean canRead() {
        return getReadStatus().allowsThis();
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return getReadStatus().allowsThis();
    }

    @Override
    public boolean canReadAll() {
        return getReadStatus().allowsAll();
    }

    @Override
    public boolean canReadProperties() {
        return getReadStatus().allowsProperties();
    }

    @Override
    public boolean isGranted(long permissions) {
        return repoPermission.isGranted(permissions);
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return repoPermission.isGranted(permissions);
    }

    private ReadStatus getReadStatus() {
        if (readStatus == null) {
            boolean canRead = repoPermission.isGranted(Permissions.READ_ACCESS_CONTROL);
            readStatus = (canRead) ? ReadStatus.ALLOW_ALL : ReadStatus.DENY_ALL;
        }
        return readStatus;
    }
}
