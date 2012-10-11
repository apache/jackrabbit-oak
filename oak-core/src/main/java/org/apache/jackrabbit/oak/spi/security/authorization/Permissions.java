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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Permissions... TODO
 */
public final class Permissions {

    public static final int NO_PERMISSION = 0;

    public static final int READ_NODE = 1;

    public static final int READ_PROPERTY = READ_NODE << 1;

    public static final int ADD_PROPERTY = READ_PROPERTY << 1;

    public static final int MODIFY_PROPERTY = ADD_PROPERTY << 1;

    public static final int REMOVE_PROPERTY = MODIFY_PROPERTY << 1;

    public static final int ADD_NODE = REMOVE_PROPERTY << 1;

    public static final int REMOVE_NODE = ADD_NODE << 1;

    public static final int READ_ACCESS_CONTROL = REMOVE_NODE << 1;

    public static final int MODIFY_ACCESS_CONTROL = READ_ACCESS_CONTROL << 1;

    public static final int NODE_TYPE_MANAGEMENT = MODIFY_ACCESS_CONTROL << 1;

    public static final int VERSION_MANAGEMENT = NODE_TYPE_MANAGEMENT << 1;

    public static final int LOCK_MANAGEMENT = VERSION_MANAGEMENT << 1;

    public static final int LIFECYCLE_MANAGEMENT = LOCK_MANAGEMENT << 1;

    public static final int RETENTION_MANAGEMENT = LIFECYCLE_MANAGEMENT << 1;

    public static final int MODIFY_CHILD_NODE_COLLECTION = RETENTION_MANAGEMENT << 1;

    public static final int NODE_TYPE_DEFINITION_MANAGEMENT = MODIFY_CHILD_NODE_COLLECTION << 1;

    public static final int NAMESPACE_MANAGEMENT = NODE_TYPE_DEFINITION_MANAGEMENT << 1;

    public static final int WORKSPACE_MANAGEMENT = NAMESPACE_MANAGEMENT << 1;

    public static final int PRIVILEGE_MANAGEMENT = WORKSPACE_MANAGEMENT << 1;

    public static final int USER_MANAGEMENT = PRIVILEGE_MANAGEMENT << 1;

    public static final int READ = READ_NODE | READ_PROPERTY;

    public static final int ALL = (READ
            | ADD_PROPERTY | MODIFY_PROPERTY | REMOVE_PROPERTY
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

    private static final Map<Integer,String> PERMISSION_NAMES = new LinkedHashMap<Integer, String>();
    static {
        PERMISSION_NAMES.put(READ_NODE, "READ_NODE");
        PERMISSION_NAMES.put(READ_PROPERTY, "READ_PROPERTY");
        PERMISSION_NAMES.put(ADD_PROPERTY, "ADD_PROPERTY");
        PERMISSION_NAMES.put(MODIFY_PROPERTY, "MODIFY_PROPERTY");
        PERMISSION_NAMES.put(REMOVE_PROPERTY, "REMOVE_PROPERTY");
        PERMISSION_NAMES.put(ADD_NODE, "ADD_NODE");
        PERMISSION_NAMES.put(REMOVE_NODE, "REMOVE_NODE");
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

    public static String getString(int permissions) {
        if (PERMISSION_NAMES.containsKey(permissions)) {
            return PERMISSION_NAMES.get(permissions);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append('|');
            for (int key : PERMISSION_NAMES.keySet()) {
                if ((permissions & key) == key) {
                    sb.append(PERMISSION_NAMES.get(key)).append('|');
                }
            }
            return sb.toString();
        }
    }

    public static boolean isRepositoryPermission(int permission) {
        return permission == NAMESPACE_MANAGEMENT ||
               permission == NODE_TYPE_DEFINITION_MANAGEMENT ||
               permission == PRIVILEGE_MANAGEMENT ||
               permission == WORKSPACE_MANAGEMENT;
    }
}
