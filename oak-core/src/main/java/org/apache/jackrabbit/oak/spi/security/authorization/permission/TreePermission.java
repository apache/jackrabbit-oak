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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The {@code TreePermission} allow to evaluate permissions defined for a given
 * {@code Tree} and it's properties.
 *
 * @see PermissionProvider#getTreePermission(org.apache.jackrabbit.oak.api.Tree, TreePermission)
 */
public interface TreePermission {

    /**
     * Retrieve the {@code TreePermission} for the tree identified by the specified
     * {@code childName} and {@code childState}, which is a child of the tree
     * associated with this instanceof {@code TreePermission}.
     *
     * @param childName The oak name of the child.
     * @param childState The child state.
     * @return The tree permission for the child tree identified by {@code childName}
     * and {@code childState}.
     */
    @Nonnull
    TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState);

    /**
     * Return if read access is granted for the {@code Tree} associated with
     * this {@code TreePermission} instance.
     *
     * @return {@code true} if the tree associated with this instance can be
     * read; {@code false} otherwise.
     */
    boolean canRead();

    /**
     * Return if read access is granted for the property of the {@code Tree} for
     * which this {@code TreePermission} instance has been created.
     *
     * @param property The property to be tested for read access.
     * @return {@code true} If the specified property can be read; {@code false} otherwise.
     */
    boolean canRead(@Nonnull PropertyState property);

    /**
     * Returns {@code true} if read access is granted to the {@code Tree} associated
     * with this instance and the whole subtree defined by it including all
     * properties. Note, that this includes access to items which require
     * specific read permissions such as e.g. {@link Permissions#READ_ACCESS_CONTROL}.
     *
     * @return {@code true} if the {@code Tree} associated with this instance as
     * well as its properties and the whole subtree can be read; {@code false} otherwise.
     */
    boolean canReadAll();

    /**
     * Returns {@code true} if all properties of the {@code Tree} associated with
     * this instance can be read.
     *
     * @return {@code true} if all properties of the {@code Tree} associated with
     * this instance can be read; {@code false} otherwise.
     */
    boolean canReadProperties();

    /**
     * Returns {@code true} if all specified permissions are granted on the
     * {@code Tree} associated with this {@code TreePermission} instance;
     * {@code false} otherwise.
     *
     * @param permissions The permissions to be tested. Note, that the implementation
     * may restrict the set of valid permissions to those that can be set and
     * evaluated for trees.
     * @return {@code true} if all permissions are granted; {@code false} otherwise.
     */
    boolean isGranted(long permissions);

    /**
     * Returns {@code true} if all specified permissions are granted on the
     * {@code PropertyState} associated with this {@code TreePermission} instance;
     * {@code false} otherwise.
     *
     * @param permissions The permissions to be tested. Note, that the implementation
     * may restrict the set of valid permissions to those that can be set and
     * evaluated for properties.
     * @param property The property state for which the permissions must be granted.
     * @return {@code true} if all permissions are granted; {@code false} otherwise.
     */
    boolean isGranted(long permissions, @Nonnull PropertyState property);

    /**
     * {@code TreePermission} which always returns {@code false} not granting
     * any permissions.
     */
    TreePermission EMPTY = new TreePermission() {
        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return EMPTY;
        }

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

        @Override
        public String toString() {
            return "TreePermission.EMPTY";
        }
    };

    /**
     * {@code TreePermission} which always returns {@code true} and thus grants
     * all permissions.
     */
    TreePermission ALL = new TreePermission() {
        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return ALL;
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return true;
        }

        @Override
        public boolean canReadAll() {
            return true;
        }

        @Override
        public boolean canReadProperties() {
            return true;
        }

        @Override
        public boolean isGranted(long permissions) {
            return true;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return true;
        }

        @Override
        public String toString() {
            return "TreePermission.ALL";
        }
    };

    TreePermission NO_RECOURSE = new TreePermission() {

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canRead() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canReadAll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canReadProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isGranted(long permissions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "TreePermission.NO_RECOURSE";
        }
    };
}