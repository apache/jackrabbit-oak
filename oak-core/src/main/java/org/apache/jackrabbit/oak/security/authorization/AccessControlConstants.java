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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;

/**
 * AccessControlConstants... TODO
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

    /**
     * @since OAK 1.0
     */
    String NT_REP_PERMISSIONS = "rep:Permissions";
    /**
     * @since OAK 1.0
     */
    String REP_PERMISSION_STORE = "rep:permissionStore";
    /**
     * @since OAK 1.0
     */
    String PERMISSIONS_STORE_PATH = JcrConstants.JCR_SYSTEM + '/' + REP_PERMISSION_STORE;

    Collection<String> POLICY_NODE_NAMES = ImmutableSet.of(REP_POLICY, REP_REPO_POLICY);

    Collection<String> ACE_PROPERTY_NAMES = ImmutableSet.of(REP_PRINCIPAL_NAME, REP_PRIVILEGES);

    Collection<String> AC_NODETYPE_NAMES = ImmutableSet.of(NT_REP_POLICY, NT_REP_ACL, NT_REP_ACE, NT_REP_DENY_ACE, NT_REP_GRANT_ACE, NT_REP_RESTRICTIONS);
}