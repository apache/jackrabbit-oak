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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;

final class Role {
    static final Role NONE = new Role(Permissions.NO_PERMISSION);
    static final Role READER = new Role(Permissions.READ, PrivilegeConstants.JCR_READ);
    static final Role EDITOR = new Role(READER,
            Permissions.WRITE|Permissions.MODIFY_CHILD_NODE_COLLECTION|Permissions.NODE_TYPE_MANAGEMENT|Permissions.VERSION_MANAGEMENT,
            PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_VERSION_MANAGEMENT);
    static final Role OWNER = new Role(EDITOR,
            Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL,
            PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL);

    private final long permissions;
    private final Set<String> privilegeNames;

    private Role(long permissions, String... privilegeNames) {
        this.permissions = permissions;
        this.privilegeNames = Collections.unmodifiableSet(CollectionUtils.toSet(privilegeNames));
    }

    private Role(@NotNull Role base, long permissions, String... privilegeNames) {
        this.permissions = base.permissions|permissions;
        this.privilegeNames = Collections.unmodifiableSet(Stream.concat(base.privilegeNames.stream(), Stream.of(privilegeNames)).collect(Collectors.toSet()));
    }

    boolean grants(long permissions) {
        return Permissions.includes(this.permissions, permissions);
    }

    Set<String> getPrivilegeNames() {
        return privilegeNames;
    }
}
