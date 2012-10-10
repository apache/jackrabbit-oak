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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserConfig provides utilities to retrieve configuration options
 * related to user management. In addition it defines some constants that
 * have been used in Jackrabbit 2.0 default user management implementation.
 */
public class UserConfig extends ConfigurationParameters {

    private static final Logger log = LoggerFactory.getLogger(UserConfig.class);

    /**
     * Configuration option defining the ID of the administrator user.
     */
    public static final String PARAM_ADMIN_ID = "adminId";

    /**
     * Configuration option defining the ID of the anonymous user. The ID
     * might be {@code null} of no anonymous user exists. In this case
     * Session#getUserID() may return {@code null} if it has been obtained
     * using {@link javax.jcr.GuestCredentials}.
     */
    public static final String PARAM_ANONYMOUS_ID = "anonymousId";

    /**
     * Configuration option to define the path underneath which user nodes
     * are being created.
     */
    public static final String PARAM_USER_PATH = "usersPath";

    /**
     * Configuration option to define the path underneath which group nodes
     * are being created.
     */
    public static final String PARAM_GROUP_PATH = "groupsPath";

    /**
     * Parameter used to change the number of levels that are used by default
     * store authorizable nodes.<br>The default number of levels is 2.
     */
    public static final String PARAM_DEFAULT_DEPTH = "defaultDepth";

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
    public static final String PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE = "groupMembershipSplitSize";

    /**
     * Configuration parameter to change the default algorithm used to generate
     * password hashes.
     */
    public static final String PARAM_PASSWORD_HASH_ALGORITHM = "passwordHashAlgorithm";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation.
     */
    public static final String PARAM_PASSWORD_HASH_ITERATIONS = "passwordHashIterations";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation.
     */
    public static final String PARAM_PASSWORD_SALT_SIZE = "passwordSaltSize";

    private final Set<AuthorizableAction> actions;

    public UserConfig() {
        this(null, null);
    }

    public UserConfig(Map<String, Object> options, Set<AuthorizableAction> actions) {
        super(options);
        this.actions = (actions == null) ? Collections.<AuthorizableAction>emptySet() : Collections.unmodifiableSet(actions);
    }

    @Nonnull
    public String getAdminId() {
        return getConfigValue(PARAM_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID);
    }

    public String getAnonymousId() {
        return getConfigValue(PARAM_ANONYMOUS_ID, UserConstants.DEFAULT_ANONYMOUS_ID);
    }

    @Nonnull
    public AuthorizableAction[] getAuthorizableActions() {
        return actions.toArray(new AuthorizableAction[actions.size()]);
    }
}