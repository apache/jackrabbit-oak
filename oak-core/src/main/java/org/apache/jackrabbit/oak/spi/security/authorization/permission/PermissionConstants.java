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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

/**
 * Implementation specific constants related to permission evaluation.
 *
 * @since OAK 1.0
 */
public interface PermissionConstants {

    String NT_REP_PERMISSIONS = "rep:Permissions";
    String NT_REP_PERMISSION_STORE = "rep:PermissionStore";
    String REP_PERMISSION_STORE = "rep:permissionStore";
    String PERMISSIONS_STORE_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + REP_PERMISSION_STORE;

    String REP_ACCESS_CONTROLLED_PATH = "rep:accessControlledPath";
	String REP_IS_ALLOW = "rep:isAllow";
    String REP_PRIVILEGE_BITS = "rep:privileges";

    Set<String> PERMISSION_NODETYPE_NAMES = ImmutableSet.of(NT_REP_PERMISSIONS, NT_REP_PERMISSION_STORE);
    Set<String> PERMISSION_NODE_NAMES = ImmutableSet.of(REP_PERMISSION_STORE);
    Set<String> PERMISSION_PROPERTY_NAMES = ImmutableSet.of(REP_ACCESS_CONTROLLED_PATH, REP_PRIVILEGE_BITS);

    /**
     * Configuration parameter to enforce backwards compatible permission
     * validation with respect to user management and node removal:
     *
     * <ul>
     *     <li>User Management: As of OAK 1.0 creation/removal of user and
     *     groups as well as modification of user/group specific protected properties
     *     requires {@link Permissions#USER_MANAGEMENT USER_MANAGEMENT}
     *     permissions while in Jackrabbit 2.0 they were covered by regular item
     *     write permissions.</li>
     *     <li>Removing Nodes: As of OAK 1.0 removing a node will succeed if the
     *     removal is granted on that specific node irrespective of the permission
     *     granted or denied within the subtree. This contrasts to JR 2.0 where
     *     removal of a node only succeeded if all child items (nodes and properties)
     *     could be removed.</li>
     * </ul>
     *
     * In order to enforce backwards compatible behavior of the listed permissions
     * above the access control configuration setup needs to contain the
     * {@code #PARAM_PERMISSIONS_JR2} configuration parameter whose value is
     * expected to be a comma separated string of permission names for which
     * backwards compatible behavior should be turned on.<p>
     *
     * Currently the following values are respected:
     * <ul>
     *     <li>"USER_MANAGEMENT" : to avoid enforcing {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#USER_MANAGEMENT}
     *     permission.</li>
     *     <li>"REMOVE_NODE" : to enforce permission checks for all items located
     *     in the subtree in case of removal.</li>
     * </ul>
     * @since OAK 1.0
     */
    String PARAM_PERMISSIONS_JR2 = "permissionsJr2";

    /**
     * Value of the {@link #PARAM_PERMISSIONS_JR2} configuration parameter that
     * contains all value entries.
     */
    String VALUE_PERMISSIONS_JR2 = Permissions.getString(Permissions.USER_MANAGEMENT | Permissions.REMOVE_NODE);

    /**
     * Configuration parameter specifying additional principals that should be
     * treated as 'administrator' thus get granted full permissions on the
     * complete repository content.
     *
     * @since OAK 1.0
     */
    String PARAM_ADMINISTRATIVE_PRINCIPALS = "administrativePrincipals";

    /**
     * Configuration parameter to enable full read access to regular nodes and
     * properties at the specified paths.
     *
     * @since OAK 1.0
     */
    String PARAM_READ_PATHS = "readPaths";

    /**
     * Default value for the {@link #PARAM_READ_PATHS} configuration parameter.
     *
     * @since OAK 1.0
     */
    Set<String> DEFAULT_READ_PATHS = ImmutableSet.of(
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH
    );
}
