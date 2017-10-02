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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;

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
     * Name of the optional multivalued access control restriction by node type name.
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#NAMES}.
     *
     * @since OAK 1.0
     */
    String REP_NT_NAMES = "rep:ntNames";

    /**
     * Name of the optional multivalued access control restriction which matches by name space prefix.
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#STRINGS}.
     *
     * @since OAK 1.0
     */
    String REP_PREFIXES = "rep:prefixes";

    /**
     * Name of the optional multivalued access control restriction by item name.
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#NAMES}.
     *
     * @since OAK 1.3.8
     */
    String REP_ITEM_NAMES = "rep:itemNames";

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

    String PARAM_RESTRICTION_PROVIDER = "restrictionProvider";
}
