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
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

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
     * <p>Name of the optional multivalued access control restriction that limits access to a single level i.e. the target 
     * node where the access control entry takes effect and optionally all or a subset of it's properties. 
     * An empty value array will make this restriction matching the target node only (i.e. equivalent to rep:glob=""). 
     * An array of property names will extend the effect of the restriction to properties of the target node that match 
     * the specified names. The {@link org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants#RESIDUAL_NAME residual name '*'}  
     * will match the target node and all it's properties.</p>
     * <p>
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#STRINGS}
     * </p>
     * <p>
     * Note: due to the support of {@link NodeTypeConstants#RESIDUAL_NAME}, which isn't a valid JCR name,
     * this restriction is defined to be of {@link org.apache.jackrabbit.oak.api.Type#STRINGS} instead of 
     * {@link org.apache.jackrabbit.oak.api.Type#NAMES}. Like the rep:glob restriction it will therefore not work with 
     * expanded JCR names or with remapped namespace prefixes.
     * </p>
     * <p>
     * Note: In case of permission evaluation for a path pointing to a non-existing JCR item (see e.g. 
     * {@link javax.jcr.Session#hasPermission(String, String)}) a best-effort attempt is made to determine if the path 
     * may point to a property, default being that the path points to a non-existing node.
     * </p>
     * 
     * Example:
     * <pre>
     * rep:current = []                => restriction applies to the target node only
     * rep:current = [*]               => restriction applies to the target node and all it's properties
     * rep:current = [jcr:primaryType] => restriction applies to the target node and it's property jcr:primaryType
     * rep:current = [a, b, prefix:c]  => restriction applies to the target node and it's properties a, b and prefix:c
     * </pre>
     * 
     * @since OAK 1.42.0
     */
    String REP_CURRENT = "rep:current";

    /**
     * <p>Name of the optional multi-valued access control restriction that allows to combine more than one 
     * {@link #REP_GLOB} restriction. The effect is equivalent to defining multiple access control entries with a 
     * single {@link #REP_GLOB} restriction each and will match a given path or item if any of the specified glob-values 
     * matches.</p>
     * <p>
     * Note, that an empty value array will never match any path/item.</p>
     * <p>
     * The corresponding restriction type is {@link org.apache.jackrabbit.oak.api.Type#STRINGS}
     * </p>
     */
    String REP_GLOBS = "rep:globs";

    /**
     * <p>Name of the optional multi-valued access control restriction that allows to limit the effect to one or multiple  
     * subtrees. It is a simplified variant of the common pattern using 2 {@link #REP_GLOB} wildcard patterns to grant or 
     * deny access on a particular node in the subtree and all its descendent items.</p>
     *
     * <pre>
     * NodePath = "/foo"
     * Restriction   |   Matches
     * -----------------------------------------------------------------------------
     * /cat          |   all descendants of /foo whose path ends with "/cat" or that have an intermediate segment /cat/
     * /cat/         |   all descendants of /foo that have an intermediate segment /cat/
     * cat           |   all siblings or descendants of /foo whose path ends with "cat" or that have an intermediate segment ending with "cat"
     * cat/          |   all siblings or descendants of /foo that have an intermediate segment ending with "cat"
     * </pre>
     *  
     * <p>Note, that variants of 'cat'-paths could also consist of multiple segments like e.g. '/cat/dog' or '/cat/dog'</p> 
     * <p>Note, that in contrast to {@link #REP_GLOB}</p> no wildcard characters are used to specify the restriction.
     * <p>Note, that an empty value array will never match any path/item.</p>
     * <p>Note, that null values and empty string values will be omitted.</p>
     */
    String REP_SUBTREES = "rep:subtrees";

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
