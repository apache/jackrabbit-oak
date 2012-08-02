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

/**
 * Permissions... TODO
 */
public final class Permissions {

    public static final int NO_PERMISSION = 0;

    public static final int READ = 1;

    public static final int ADD_PROPERTY = READ << 1;

    public static final int MODIFY_PROPERTY = ADD_PROPERTY << 1;

    public static final int REMOVE_PROPERTY = MODIFY_PROPERTY << 1;

    public static final int ADD_NODE = REMOVE_PROPERTY << 1;

    public static final int REMOVE_NODE = ADD_NODE << 1;

    public static final int READ_ACCESS_CONTROL = REMOVE_NODE << 1;

    public static final int MODIFY_ACCESS_CONTROL = READ_ACCESS_CONTROL << 1;

    public static final int NODE_TYPE_MANAGEMENT = MODIFY_ACCESS_CONTROL << 1;

    public static final int VERSION_MANAGEMENT = NODE_TYPE_MANAGEMENT << 1;

    public static final int LOCK_MANAGEMENT = VERSION_MANAGEMENT << 1;

    public static final int MODIFY_CHILD_NODE_COLLECTION = LOCK_MANAGEMENT << 1;

    public static final int NODE_TYPE_DEFINITION_MANAGEMENT = MODIFY_CHILD_NODE_COLLECTION << 1;

    public static final int NAMESPACE_MANAGEMENT = NODE_TYPE_DEFINITION_MANAGEMENT << 1;

    public static final int WORKSPACE_MANAGEMENT = NAMESPACE_MANAGEMENT << 1;

    public static final int PRIVILEGE_MANAGEMENT = WORKSPACE_MANAGEMENT << 1;

    public static final int ALL = (READ | ADD_PROPERTY | MODIFY_PROPERTY
            | ADD_NODE | REMOVE_NODE
            | REMOVE_PROPERTY | READ_ACCESS_CONTROL | MODIFY_ACCESS_CONTROL | NODE_TYPE_MANAGEMENT
            | VERSION_MANAGEMENT | LOCK_MANAGEMENT | MODIFY_CHILD_NODE_COLLECTION
            | NODE_TYPE_DEFINITION_MANAGEMENT | NAMESPACE_MANAGEMENT | WORKSPACE_MANAGEMENT | PRIVILEGE_MANAGEMENT);
}
