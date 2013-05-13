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
package org.apache.jackrabbit.oak.security.authorization;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

/**
 * Constants for this access control management implementation.
 */
public interface AccessControlConstants {

    String REP_POLICY = "rep:policy";
    String REP_REPO_POLICY = "rep:repoPolicy";
    String REP_PRIVILEGES = "rep:privileges";
    String REP_PRINCIPAL_NAME = "rep:principalName";
    String REP_GLOB = "rep:glob";
    String REP_NODE_PATH = "rep:nodePath";
    /**
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
     * validation with respect to user/group creation, modification and removal.
     * As of OAK 1.0 those actions require
     * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#USER_MANAGEMENT USER_MANAGEMENT}
     * permissions while in Jackrabbit 2.0 they were covered by regular item
     * write permissions.
     *
     * @since OAK 1.0
     */
    String PARAM_PERMISSIONS_JR2 = "permissionsJr2";

    /**
     * Configuration parameter to enable full read access to regular nodes and
     * properties at the specified paths.
     */
    String PARAM_READ_PATHS = "readPaths";

    /**
     * Default value for the {@link #PARAM_READ_PATHS} configuration parameter.
     */
    Set<String> DEFAULT_READ_PATHS = ImmutableSet.of(
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH);
}
