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

import java.util.Set;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;

public interface ThreeRolesConstants {

    String MIX_REP_THREE_ROLES_POLICY = "rep:ThreeRolesMixin";
    String NT_REP_THREE_ROLES_POLICY = "rep:ThreeRolesPolicy";

    String REP_3_ROLES_POLICY = "rep:threeRolesPolicy";

    String REP_READERS = "rep:readers";
    String REP_EDITORS = "rep:editors";
    String REP_OWNERS = "rep:owners";

    Set<String> NAMES = Set.of(REP_3_ROLES_POLICY, REP_READERS, REP_EDITORS, REP_OWNERS);

    long SUPPORTED_PERMISSIONS =
                    Permissions.READ |
                    Permissions.MODIFY_CHILD_NODE_COLLECTION |
                    Permissions.WRITE |
                    Permissions.NODE_TYPE_MANAGEMENT |
                    Permissions.VERSION_MANAGEMENT |
                    Permissions.READ_ACCESS_CONTROL |
                    Permissions.MODIFY_ACCESS_CONTROL;
}