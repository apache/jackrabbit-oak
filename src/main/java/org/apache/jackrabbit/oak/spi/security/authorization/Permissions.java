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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.TreeLocation;

/**
 * Permissions... TODO
 */
public final class Permissions {

    private Permissions() {
    }

    public static final long NO_PERMISSION = 0;

    public static final long READ_NODE = 1;

    public static final long READ_PROPERTY = READ_NODE << 1;

    public static final long ADD_PROPERTY = READ_PROPERTY << 1;

    public static final long MODIFY_PROPERTY = ADD_PROPERTY << 1;

    public static final long REMOVE_PROPERTY = MODIFY_PROPERTY << 1;

    public static final long ADD_NODE = REMOVE_PROPERTY << 1;

    public static final long REMOVE_NODE = ADD_NODE << 1;

    public static final long READ_ACCESS_CONTROL = REMOVE_NODE << 1;

    public static final long MODIFY_ACCESS_CONTROL = READ_ACCESS_CONTROL << 1;

    public static final long NODE_TYPE_MANAGEMENT = MODIFY_ACCESS_CONTROL << 1;

    public static final long VERSION_MANAGEMENT = NODE_TYPE_MANAGEMENT << 1;

    public static final long LOCK_MANAGEMENT = VERSION_MANAGEMENT << 1;

    public static final long LIFECYCLE_MANAGEMENT = LOCK_MANAGEMENT << 1;

    public static final long RETENTION_MANAGEMENT = LIFECYCLE_MANAGEMENT << 1;

    public static final long MODIFY_CHILD_NODE_COLLECTION = RETENTION_MANAGEMENT << 1;

    public static final long NODE_TYPE_DEFINITION_MANAGEMENT = MODIFY_CHILD_NODE_COLLECTION << 1;

    public static final long NAMESPACE_MANAGEMENT = NODE_TYPE_DEFINITION_MANAGEMENT << 1;

    public static final long WORKSPACE_MANAGEMENT = NAMESPACE_MANAGEMENT << 1;

    public static final long PRIVILEGE_MANAGEMENT = WORKSPACE_MANAGEMENT << 1;

    public static final long USER_MANAGEMENT = PRIVILEGE_MANAGEMENT << 1;

    public static final long READ = READ_NODE | READ_PROPERTY;

    public static final long REMOVE = REMOVE_NODE | REMOVE_PROPERTY;

    public static final long SET_PROPERTY = ADD_PROPERTY | MODIFY_PROPERTY | REMOVE_PROPERTY;

    public static final long ALL = (READ
            | SET_PROPERTY
            | ADD_NODE | REMOVE_NODE
            | READ_ACCESS_CONTROL | MODIFY_ACCESS_CONTROL
            | NODE_TYPE_MANAGEMENT
            | VERSION_MANAGEMENT
            | LOCK_MANAGEMENT
            | LIFECYCLE_MANAGEMENT
            | RETENTION_MANAGEMENT
            | MODIFY_CHILD_NODE_COLLECTION
            | NODE_TYPE_DEFINITION_MANAGEMENT
            | NAMESPACE_MANAGEMENT
            | WORKSPACE_MANAGEMENT
            | PRIVILEGE_MANAGEMENT
            | USER_MANAGEMENT
    );

    private static final Map<Long, String> PERMISSION_NAMES = new LinkedHashMap<Long, String>();

    static {
        PERMISSION_NAMES.put(ALL, "ALL");
        PERMISSION_NAMES.put(READ, "READ");
        PERMISSION_NAMES.put(READ_NODE, "READ_NODE");
        PERMISSION_NAMES.put(READ_PROPERTY, "READ_PROPERTY");
        PERMISSION_NAMES.put(SET_PROPERTY, "SET_PROPERTY");
        PERMISSION_NAMES.put(ADD_PROPERTY, "ADD_PROPERTY");
        PERMISSION_NAMES.put(MODIFY_PROPERTY, "MODIFY_PROPERTY");
        PERMISSION_NAMES.put(REMOVE_PROPERTY, "REMOVE_PROPERTY");
        PERMISSION_NAMES.put(ADD_NODE, "ADD_NODE");
        PERMISSION_NAMES.put(REMOVE_NODE, "REMOVE_NODE");
        PERMISSION_NAMES.put(REMOVE, "REMOVE");
        PERMISSION_NAMES.put(MODIFY_CHILD_NODE_COLLECTION, "MODIFY_CHILD_NODE_COLLECTION");
        PERMISSION_NAMES.put(READ_ACCESS_CONTROL, "READ_ACCESS_CONTROL");
        PERMISSION_NAMES.put(MODIFY_ACCESS_CONTROL, "MODIFY_ACCESS_CONTROL");
        PERMISSION_NAMES.put(NODE_TYPE_MANAGEMENT, "NODE_TYPE_MANAGEMENT");
        PERMISSION_NAMES.put(VERSION_MANAGEMENT, "VERSION_MANAGEMENT");
        PERMISSION_NAMES.put(LOCK_MANAGEMENT, "LOCK_MANAGEMENT");
        PERMISSION_NAMES.put(LIFECYCLE_MANAGEMENT, "LIFECYCLE_MANAGEMENT");
        PERMISSION_NAMES.put(RETENTION_MANAGEMENT, "RETENTION_MANAGEMENT");
        PERMISSION_NAMES.put(NODE_TYPE_DEFINITION_MANAGEMENT, "NODE_TYPE_DEFINITION_MANAGEMENT");
        PERMISSION_NAMES.put(NAMESPACE_MANAGEMENT, "NAMESPACE_MANAGEMENT");
        PERMISSION_NAMES.put(WORKSPACE_MANAGEMENT, "WORKSPACE_MANAGEMENT");
        PERMISSION_NAMES.put(PRIVILEGE_MANAGEMENT, "PRIVILEGE_MANAGEMENT");
        PERMISSION_NAMES.put(USER_MANAGEMENT, "USER_MANAGEMENT");
    }

    public static String getString(long permissions) {
        if (PERMISSION_NAMES.containsKey(permissions)) {
            return PERMISSION_NAMES.get(permissions);
        } else {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Long, String> entry : PERMISSION_NAMES.entrySet()) {
                long key = entry.getKey();
                if ((permissions & key) == key) {
                    if (sb.length() != 0) {
                        sb.append(',');
                    }
                    sb.append(entry.getValue());
                }
            }
            return sb.toString();
        }
    }

    public static boolean isRepositoryPermission(long permission) {
        return permission == NAMESPACE_MANAGEMENT ||
                permission == NODE_TYPE_DEFINITION_MANAGEMENT ||
                permission == PRIVILEGE_MANAGEMENT ||
                permission == WORKSPACE_MANAGEMENT;
    }

    public static boolean includes(long permissions, long permissionsToTest) {
        return (permissions & permissionsToTest) == permissionsToTest;
    }

    public static long getPermissions(String jcrActions, TreeLocation location) {
        Set<String> s = new HashSet<String>(Arrays.asList(jcrActions.split(",")));
        int permissions = 0;
        if (s.remove(Session.ACTION_READ)) {
            permissions |= READ;
        }
        if (s.remove(Session.ACTION_ADD_NODE)) {
            permissions |= ADD_NODE;
        }
        if (s.remove(Session.ACTION_SET_PROPERTY)) {
            permissions |= ADD_PROPERTY | MODIFY_PROPERTY;
        }
        if (s.remove(Session.ACTION_REMOVE)) {
            if (!location.exists()) {
                permissions |= REMOVE;
            } else if (location.getProperty() == null) {
                permissions |= REMOVE_NODE;
            } else {
                permissions |= REMOVE_PROPERTY;
            }
        }
        if (!s.isEmpty()) {
            throw new IllegalArgumentException("Unknown actions: " + s);
        }
        return permissions;
    }
}