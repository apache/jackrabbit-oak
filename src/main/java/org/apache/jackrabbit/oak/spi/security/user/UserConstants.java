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
package org.apache.jackrabbit.oak.spi.security.user;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;

/**
 * UserConstants...
 */
public interface UserConstants {

    String NT_REP_AUTHORIZABLE = "rep:Authorizable";
    String NT_REP_AUTHORIZABLE_FOLDER = "rep:AuthorizableFolder";
    String NT_REP_USER = "rep:User";
    String NT_REP_GROUP = "rep:Group";
    String NT_REP_MEMBERS = "rep:Members";
    String MIX_REP_IMPERSONATABLE = "rep:Impersonatable";
    String REP_PRINCIPAL_NAME = "rep:principalName";
    String REP_AUTHORIZABLE_ID = "rep:authorizableId";
    String REP_PASSWORD = "rep:password";
    String REP_DISABLED = "rep:disabled";
    String REP_MEMBERS = "rep:members";
    String REP_IMPERSONATORS = "rep:impersonators";

    Collection<String> GROUP_PROPERTY_NAMES = ImmutableSet.of(
            REP_PRINCIPAL_NAME,
            REP_AUTHORIZABLE_ID,
            REP_MEMBERS
    );

    Collection<String> USER_PROPERTY_NAMES = ImmutableSet.of(
            REP_PRINCIPAL_NAME,
            REP_AUTHORIZABLE_ID,
            REP_PASSWORD,
            REP_DISABLED,
            REP_IMPERSONATORS
    );

    /**
     * Configuration option defining the ID of the administrator user.
     */
    String PARAM_ADMIN_ID = "adminId";

    /**
     * Default value for {@link #PARAM_ADMIN_ID}
     */
    String DEFAULT_ADMIN_ID = "admin";

    /**
     * Configuration option defining the ID of the anonymous user. The ID
     * might be {@code null} of no anonymous user exists. In this case
     * Session#getUserID() may return {@code null} if it has been obtained
     * using {@link javax.jcr.GuestCredentials}.
     */
    String PARAM_ANONYMOUS_ID = "anonymousId";

    /**
     * Default value for {@link #PARAM_ANONYMOUS_ID}
     */
    String DEFAULT_ANONYMOUS_ID = "anonymous";

    /**
     * Configuration option to define the path underneath which user nodes
     * are being created.
     */
    String PARAM_USER_PATH = "usersPath";

    /**
     * Default value for {@link #PARAM_USER_PATH}
     */
    String DEFAULT_USER_PATH = "/rep:security/rep:authorizables/rep:users";

    /**
     * Configuration option to define the path underneath which group nodes
     * are being created.
     */
    String PARAM_GROUP_PATH = "groupsPath";

    /**
     * Default value for {@link #PARAM_GROUP_PATH}
     */
    String DEFAULT_GROUP_PATH = "/rep:security/rep:authorizables/rep:groups";

    /**
     * Parameter used to change the number of levels that are used by default
     * store authorizable nodes.<br>The default number of levels is 2.
     */
    String PARAM_DEFAULT_DEPTH = "defaultDepth";

    /**
     * Default value for {@link #PARAM_DEFAULT_DEPTH}
     */
    int DEFAULT_DEPTH = 2;

    /**
     * Its value determines the maximum number of members within a given
     * content structure until additional intermediate structuring is being
     * added. This may for example be used to
     * <ul>
     *     <li>switch storing group members in JCR properties or nodes</li>
     *     <li>define maximum number of members is a multivalued property</li>
     *     <li>define maximum number of member properties within a given
     *     node structure</li>
     * </ul>
     */
    String PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE = "groupMembershipSplitSize";

    /**
     * Configuration parameter to change the default algorithm used to generate
     * password hashes.
     */
    String PARAM_PASSWORD_HASH_ALGORITHM = "passwordHashAlgorithm";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation.
     */
    String PARAM_PASSWORD_HASH_ITERATIONS = "passwordHashIterations";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation.
     */
    String PARAM_PASSWORD_SALT_SIZE = "passwordSaltSize";

    /**
     * Optional configuration parameter defining how to generate the name of the
     * authorizable node from the ID of the new authorizable that is being created.
     * The value is expected to be an instance of {@link AuthorizableNodeName}.
     * By default {@link AuthorizableNodeName#DEFAULT} is used.
     */
    String PARAM_AUTHORIZABLE_NODE_NAME = "authorizableNodeName";

    /**
     * Optional configuration parameter to set the
     * {@link org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider}
     * to be used with the given user management implementation.
     * Unless otherwise specified in the configuration
     * {@link org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider}
     * is used.
     */
    String PARAM_AUTHORIZABLE_ACTION_PROVIDER = "authorizableActionProvider";
}
