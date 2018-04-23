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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class ThreeRolesTreePermission implements TreePermission, ThreeRolesConstants {

    private final Role role;
    private final boolean isAcContent;

    ThreeRolesTreePermission(@Nonnull Role role, boolean isAcContent) {
        this.role = role;
        this.isAcContent = isAcContent;
    }

    @Nonnull
    Role getRole() {
        return role;
    }

    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        if (isAcContent) {
            return this;
        } else {
            // EXERCISE: respect access controlled content defined by other modules
            return new ThreeRolesTreePermission(role, REP_3_ROLES_POLICY.equals(childName));
        }
    }

    @Override
    public boolean canRead() {
        if (isAcContent) {
            return isGranted(Permissions.READ_ACCESS_CONTROL);
        } else {
            return isGranted(Permissions.READ);
        }
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return canRead();
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        return canRead();
    }

    @Override
    public boolean isGranted(long permissions) {
        return role.grants(permissions);
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return isGranted(permissions);
    }
}
