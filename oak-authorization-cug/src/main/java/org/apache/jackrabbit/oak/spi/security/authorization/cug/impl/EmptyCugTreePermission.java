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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;

/**
 * Same as {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission#EMPTY}
 * from a permission point of view but indicating that it refers to a tree that
 * potentially contains a CUG in the subtree thus forcing continued evaluation,
 * where as {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission#EMPTY}
 * indicates that this permission model will never grant access in the subtree
 * and thus can be ignored.
 */
final class EmptyCugTreePermission extends AbstractTreePermission {

    EmptyCugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull CugPermissionProvider permissionProvider) {
        super(tree, type, permissionProvider);
    }

    //-----------------------------------------------------< TreePermission >---

    @Override
    public boolean canRead() {
        return false;
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return false;
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        return false;
    }

    @Override
    public boolean isGranted(long permissions) {
        return false;
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return false;
    }
}