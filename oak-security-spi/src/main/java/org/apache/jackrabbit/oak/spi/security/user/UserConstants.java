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
 * User management related constants. Please note that all names and paths
 * are OAK names/paths and therefore are not suited to be used in JCR context
 * with remapped namespaces.
 */
public interface UserConstants {

    String NT_REP_AUTHORIZABLE = "rep:Authorizable";
    String NT_REP_AUTHORIZABLE_FOLDER = "rep:AuthorizableFolder";
    String NT_REP_USER = "rep:User";
    String NT_REP_GROUP = "rep:Group";
    String NT_REP_SYSTEM_USER = "rep:SystemUser";
    String NT_REP_PASSWORD = "rep:Password";
    @Deprecated
    String NT_REP_MEMBERS = "rep:Members";
    String NT_REP_MEMBER_REFERENCES_LIST = "rep:MemberReferencesList";
    String NT_REP_MEMBER_REFERENCES = "rep:MemberReferences";

    String MIX_REP_IMPERSONATABLE = "rep:Impersonatable";

    String REP_PRINCIPAL_NAME = "rep:principalName";
    String REP_AUTHORIZABLE_ID = "rep:authorizableId";
    String REP_PASSWORD = "rep:password";
    String REP_PASSWORD_LAST_MODIFIED = "rep:passwordLastModified";
    String REP_DISABLED = "rep:disabled";
    String REP_MEMBERS = "rep:members";
    String REP_MEMBERS_LIST = "rep:membersList";
    String REP_IMPERSONATORS = "rep:impersonators";
    String REP_PWD = "rep:pwd";
    String REP_PWD_HISTORY = "rep:pwdHistory";

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

    Collection<String> PWD_PROPERTY_NAMES = ImmutableSet.of(
            REP_PASSWORD_LAST_MODIFIED
    );

    Collection<String> NT_NAMES = ImmutableSet.of(
            NT_REP_USER, NT_REP_GROUP, NT_REP_PASSWORD,
            NT_REP_MEMBERS, NT_REP_MEMBER_REFERENCES, NT_REP_MEMBER_REFERENCES_LIST);

    /**
     * Configuration option defining the ID of the administrator user.
     */
    String PARAM_ADMIN_ID = "adminId";

    /**
     * Configuration option defining if the admin password should be omitted
     * upon user creation.
     */
    String PARAM_OMIT_ADMIN_PW = "omitAdminPw";

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
     * Mandatory configuration option denoting the user {@link org.apache.jackrabbit.oak.spi.security.authentication.Authentication} implementation to use in the login module.
     */
    String PARAM_USER_AUTHENTICATION_FACTORY = "userAuthenticationFactory";

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
     * Configuration option to define the path relative to the user root node
     * underneath which system user nodes are being created.
     */
    String PARAM_SYSTEM_RELATIVE_PATH = "systemRelativePath";

    /**
     * Default intermediate path for system users.
     */
    String DEFAULT_SYSTEM_RELATIVE_PATH = "system";

    /**
     * Parameter used to change the number of levels that are used by default to
     * store authorizable nodes.<br>The default number of levels is 2.
     */
    String PARAM_DEFAULT_DEPTH = "defaultDepth";

    /**
     * Default value for {@link #PARAM_DEFAULT_DEPTH}
     */
    int DEFAULT_DEPTH = 2;

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
     * Optionally enables the UsernameCaseMapped profile defined in
     * https://tools.ietf.org/html/rfc7613#section-3.2 for user name comparison.
     * Use this if half-width and full-width user names should be considered
     * equal.
     */
    String PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE = "enableRFC7613UsercaseMappedProfile";

    /**
     * Default value for {@link #PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE}
     */
    boolean DEFAULT_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE = false;

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

    /**
     * Optional configuration parameter that might be used to get back support
     * for the auto-save behavior which has been dropped in the default
     * user management implementation present with OAK.
     *
     * <p>Note that this option has been added for those cases where API consumers
     * rely on the implementation specific behavior present with Jackrabbit 2.x.
     * In general using this option should not be required as the Jackrabbit
     * User Management API expects that API consumers tests the auto-save
     * mode is enabled. Therefore this option should be considered a temporary
     * workaround after upgrading a repository to OAK; the affected code should
     * be reviewed and adjusted accordingly.</p>
     */
    String PARAM_SUPPORT_AUTOSAVE = "supportAutoSave";

    /**
     * Optional configuration parameter indicating the maximum age in days a password may have
     * before it expires. If the value specified is {@code > 0}, password expiry is implicitly enabled.
     */
    String PARAM_PASSWORD_MAX_AGE = "passwordMaxAge";

    /**
     * Default value for {@link #PARAM_PASSWORD_MAX_AGE}
     */
    int DEFAULT_PASSWORD_MAX_AGE = 0;

    /**
     * Optional configuration parameter indicating whether users must change their passwords
     * on first login. If enabled, passwords are immediately expired upon user creation.
     */
    String PARAM_PASSWORD_INITIAL_CHANGE = "initialPasswordChange";

    /**
     * Default value for {@link #PARAM_PASSWORD_INITIAL_CHANGE}
     */
    boolean DEFAULT_PASSWORD_INITIAL_CHANGE = false;

    /**
     * Name of the {@link javax.jcr.SimpleCredentials} attribute containing the new password.
     * This may be used change the password via the credentials object.
     */
    String CREDENTIALS_ATTRIBUTE_NEWPASSWORD = "user.newpassword";

    /**
     * Optional configuration parameter indicating the maximum number of passwords recorded for a user after
     * password changes. If the value specified is {@code > 0}, password history checking during password change is implicitly
     * enabled and the new password provided during a password change must not be found in the already recorded
     * history.
     *
     * @since Oak 1.3.3
     */
    String PARAM_PASSWORD_HISTORY_SIZE = "passwordHistorySize";

    /**
     * Constant to indicate disabled password history, which is the default.
     *
     * @since Oak 1.3.3
     */
    int PASSWORD_HISTORY_DISABLED_SIZE = 0;
}
