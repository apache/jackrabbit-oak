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

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

/**
 * Constants for the default access control management implementation and
 * and for built-in access control related node types.
 */
public interface AccessControlConstants {

    String REP_POLICY = "rep:policy";
    String REP_REPO_POLICY = "rep:repoPolicy";
    String REP_PRIVILEGES = "rep:privileges";
    String REP_PRINCIPAL_NAME = "rep:principalName";
    String REP_GLOB = "rep:glob";
    String REP_NODE_PATH = "rep:nodePath";

    /**
     * Name of the optional access control restriction by node type name.
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#NAMES}.
     *
     * @since OAK 1.0
     */
    String REP_NT_NAMES = "rep:ntNames";
    /**
     * @since OAK 1.0
     */
    String REP_RESTRICTIONS = "rep:restrictions";

    String MIX_REP_ACCESS_CONTROLLABLE = "rep:AccessControllable";
    String MIX_REP_REPO_ACCESS_CONTROLLABLE = "rep:RepoAccessControllable";
    String NT_REP_POLICY = "rep:Policy";
    String NT_REP_ACL = "rep:ACL";
    String NT_REP_ACE = "rep:ACE";
    String NT_REP_GRANT_ACE = "rep:GrantACE";
    String NT_REP_DENY_ACE = "rep:DenyACE";

    /**
     * @since OAK 1.0
     */
    String NT_REP_RESTRICTIONS = "rep:Restrictions";

    Collection<String> POLICY_NODE_NAMES = ImmutableSet.of(REP_POLICY, REP_REPO_POLICY);

    Collection<String> ACE_PROPERTY_NAMES = ImmutableSet.of(REP_PRINCIPAL_NAME, REP_PRIVILEGES);

    Collection<String> AC_NODETYPE_NAMES = ImmutableSet.of(NT_REP_POLICY, NT_REP_ACL, NT_REP_ACE, NT_REP_DENY_ACE, NT_REP_GRANT_ACE, NT_REP_RESTRICTIONS);

    /**
     * Configuration parameter to enforce backwards compatible permission
     * validation with respect to user management and node removal:
     *
     * <ul>
     *     <li>User Management: As of OAK 1.0 creation/removal of user and
     *     groups as well as modification of user/group specific protected properties
     *     requires {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#USER_MANAGEMENT USER_MANAGEMENT}
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
     *     <li>"USER_MANAGEMENT" : to avoid enforcing {@link Permissions#USER_MANAGEMENT}
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
     * Configuration parameter to enable full read access to regular nodes and
     * properties at the specified paths.
     */
    String PARAM_READ_PATHS = "readPaths";

    /**
     * Default value for the {@link #PARAM_READ_PATHS} configuration parameter.
     */
    String[] DEFAULT_READ_PATHS = new String[] {
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH
    };

    /**
     * Configuration parameter specifying additional principals that should be
     * treated as 'administrator' thus get granted full permissions on the
     * complete repository content.
     */
    String PARAM_ADMINISTRATOR_PRINCIPALS = "administratorPrincipals";
}
