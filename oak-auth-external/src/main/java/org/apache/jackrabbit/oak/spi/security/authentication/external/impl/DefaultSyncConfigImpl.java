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

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.AttributeDefinition;

/**
 * {@code DefaultSyncConfig} defines how users and groups from an external source are synced into the repository using
 * the {@link DefaultSyncHandler}.
 */
public class DefaultSyncConfigImpl extends DefaultSyncConfig {

    /**
     * @see DefaultSyncConfig#getName()
     */
    public static final String PARAM_NAME = "handler.name";
    /**
     * @see DefaultSyncConfig#getName()
     */
    public static final String PARAM_NAME_DEFAULT = DefaultSyncConfig.DEFAULT_NAME;

    /**
     * @see DefaultSyncConfig.User#getExpirationTime()
     */
    public static final String PARAM_USER_EXPIRATION_TIME = "user.expirationTime";
    /**
     * @see DefaultSyncConfig.User#getExpirationTime()
     */
    public static final String PARAM_USER_EXPIRATION_TIME_DEFAULT = "1h";

    /**
     * @see DefaultSyncConfig.User#getAutoMembership()
     */
    public static final String PARAM_USER_AUTO_MEMBERSHIP = "user.autoMembership";
    /**
     * @see DefaultSyncConfig.User#getAutoMembership()
     */
    public static final String[] PARAM_USER_AUTO_MEMBERSHIP_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.User#getPropertyMapping()
     */
    public static final String PARAM_USER_PROPERTY_MAPPING = "user.propertyMapping";
    /**
     * @see DefaultSyncConfig.User#getPropertyMapping()
     */
    public static final String[] PARAM_USER_PROPERTY_MAPPING_DEFAULT = {"rep:fullname=cn"};

    /**
     * @see DefaultSyncConfig.User#getPathPrefix()
     */
    public static final String PARAM_USER_PATH_PREFIX = "user.pathPrefix";
    /**
     * @see DefaultSyncConfig.User#getPathPrefix()
     */
    public static final String PARAM_USER_PATH_PREFIX_DEFAULT = "";

    /**
     * @see DefaultSyncConfig.User#getMembershipExpirationTime()
     */
    public static final String PARAM_USER_MEMBERSHIP_EXPIRATION_TIME = "user.membershipExpTime";
    /**
     * @see DefaultSyncConfig.User#getMembershipExpirationTime()
     */
    public static final String PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT = "1h";

    /**
     * @see DefaultSyncConfig.User#getMembershipNestingDepth()
     */
    public static final String PARAM_USER_MEMBERSHIP_NESTING_DEPTH = "user.membershipNestingDepth";
    /**
     * @see DefaultSyncConfig.User#getMembershipNestingDepth()
     */
    public static final int PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT = 0;

    /**
     * Configuration option to enable dynamic group membership. If enabled the
     * implementation will no longer synchronized group accounts into the repository
     * but instead will enable a dedicated principal management: This results in
     * external users having their complete principal set as defined external IDP
     * synchronized to the repository asserting proper population of the
     * {@link javax.security.auth.Subject} upon login. Please note that the external
     * groups are reflected through the built-in principal management and thus can
     * be retrieved for authorization purposes. However, the information is no
     * longer reflected through the Jackrabbit user management API.
     *
     * @see DefaultSyncConfig.User#getDynamicMembership()
     */
    public static final String PARAM_USER_DYNAMIC_MEMBERSHIP = "user.dynamicMembership";
    /**
     * @see DefaultSyncConfig.User#getDynamicMembership()
     */
    public static final boolean PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT = false;

    /**
     * Configuration option to enforce dynamic group membership upon user sync. If enabled the
     * implementation will clean up previous synchronized membership information that is not yet dynamic.
     *
     * @see DefaultSyncConfig.User#getDynamicMembership()
     */
    public static final String PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP = "user.enforceDynamicMembership";
    /**
     * @see DefaultSyncConfig.User#getDynamicMembership()
     */
    public static final boolean PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP_DEFAULT = false;

    /**
     * @see User#getDisableMissing()
     */
    public static final String PARAM_DISABLE_MISSING_USERS = "user.disableMissing";
    /**
     * @see User#getDisableMissing()
     */
    public static final boolean PARAM_DISABLE_MISSING_USERS_DEFAULT = false;

    /**
     * @see DefaultSyncConfig.Group#getExpirationTime()
     */
    public static final String PARAM_GROUP_EXPIRATION_TIME = "group.expirationTime";
    /**
     * @see DefaultSyncConfig.Group#getExpirationTime()
     */
    public static final String PARAM_GROUP_EXPIRATION_TIME_DEFAULT = "1d";

    /**
     * @see DefaultSyncConfig.Group#getAutoMembership()
     */
    public static final String PARAM_GROUP_AUTO_MEMBERSHIP = "group.autoMembership";
    /**
     * @see DefaultSyncConfig.Group#getAutoMembership()
     */
    public static final String[] PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.Group#getPropertyMapping()
     */
    public static final String PARAM_GROUP_PROPERTY_MAPPING = "group.propertyMapping";
    /**
     * @see DefaultSyncConfig.Group#getPropertyMapping()
     */
    public static final String[] PARAM_GROUP_PROPERTY_MAPPING_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.Group#getPathPrefix()
     */
    public static final String PARAM_GROUP_PATH_PREFIX = "group.pathPrefix";
    /**
     * @see DefaultSyncConfig.Group#getPathPrefix()
     */
    public static final String PARAM_GROUP_PATH_PREFIX_DEFAULT = "";

    /**
     * @see DefaultSyncConfig.Group#getDynamicGroups()
     */
    public static final String PARAM_GROUP_DYNAMIC_GROUPS = "group.dynamicGroups";
    /**
     * @see DefaultSyncConfig.Group#getDynamicGroups()
     */
    public static final boolean PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT = false;

    /**
     * @see Authorizable#isApplyRFC7613UsernameCaseMapped()
     */
    public static final String PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE = "enableRFC7613UsercaseMappedProfile";
    /**
     * @see Authorizable#isApplyRFC7613UsernameCaseMapped()
     */
    public static final boolean PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT = false;

    private static final long MILLIS_PER_HOUR = 60 * 60 * 1000L;
    private static final ConfigurationParameters.Milliseconds ONE_HOUR = ConfigurationParameters.Milliseconds.of(MILLIS_PER_HOUR);
    private static final ConfigurationParameters.Milliseconds ONE_DAY = ConfigurationParameters.Milliseconds.of(24 * MILLIS_PER_HOUR);

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler",
            name = "Apache Jackrabbit Oak Default Sync Handler"
    )
    @interface Configuration {
        /**
         * @see DefaultSyncConfig#getName()
         */
        @AttributeDefinition(
                name = "Sync Handler Name",
                description = "Name of this sync configuration. This is used to reference this handler by the login modules."
        )
        String handler_name() default PARAM_NAME_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getExpirationTime()
         */
        @AttributeDefinition(
                name = "User Expiration Time",
                description = "Duration until a synced user gets expired (eg. '1h 30m' or '1d')."
        )
        String user_expirationTime() default PARAM_USER_EXPIRATION_TIME_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getAutoMembership()
         */
        @AttributeDefinition(
                name = "User auto membership",
                description = "List of groups that a synced user is added to automatically",
                cardinality = Integer.MAX_VALUE
        )
        String[] user_autoMembership();

        /**
         * @see DefaultSyncConfig.User#getPropertyMapping()
         */
        @AttributeDefinition(
                name = "User property mapping",
                description = "List mapping definition of local properties from external ones. eg: 'profile/email=mail'." +
                        "Use double quotes for fixed values. eg: 'profile/nt:primaryType=\"nt:unstructured\"",
                cardinality = Integer.MAX_VALUE
        )
        String[] user_propertyMapping() default {"rep:fullname=cn"};

        /**
         * @see DefaultSyncConfig.User#getPathPrefix()
         */
        @AttributeDefinition(
                name = "User Path Prefix",
                description = "The path prefix used when creating new users."
        )
        String user_pathPrefix() default PARAM_USER_PATH_PREFIX_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getMembershipExpirationTime()
         */
        @AttributeDefinition(
                name = "User Membership Expiration",
                description = "Time after which membership expires (eg. '1h 30m' or '1d'). Note however, that a membership sync is aways bound to a sync of the user."
        )
        String user_membershipExpTime() default PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getMembershipNestingDepth()
         */
        @AttributeDefinition(
                name = "User membership nesting depth",
                description = "Returns the maximum depth of group nesting when membership relations are synced. " +
                        "A value of 0 effectively disables group membership lookup. A value of 1 only adds the direct " +
                        "groups of a user. This value has no effect when syncing individual groups only when syncing a " +
                        "users membership ancestry."
        )
        int user_membershipNestingDepth() default PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getDynamicMembership()
         */
        @AttributeDefinition(
                name = "User Dynamic Membership",
                description = "If enabled membership of external identities (user) is no longer fully reflected " +
                        "within the repositories user management."
        )
        boolean user_dynamicMembership() default PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT;

        /**
         * @see DefaultSyncConfig.User#getDynamicMembership()
         */
        @AttributeDefinition(
                name = "User Enforce Dynamic Membership",
                description = "If enabled dynamic membership will be enforced for previously synchronized users. Note, that this option has no effect if 'dynamic membership' is disabled."
        )
        boolean user_enforceDynamicMembership() default PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP_DEFAULT;

        /**
         * @see User#getDisableMissing()
         */
        @AttributeDefinition(
                name = "Disable missing users",
                description = "If true, users that no longer exist on the external provider will be locally disabled, " +
                        "and re-enabled if they become valid again. If false (default) they will be removed."
        )
        boolean user_disableMissing() default PARAM_DISABLE_MISSING_USERS_DEFAULT;

        /**
         * @see DefaultSyncConfig.Group#getExpirationTime()
         */
        @AttributeDefinition(
                name = "Group Expiration Time",
                description = "Duration until a synced group expires (eg. '1h 30m' or '1d')."
        )
        String group_expirationTime() default PARAM_GROUP_EXPIRATION_TIME_DEFAULT;

        /**
         * @see DefaultSyncConfig.Group#getAutoMembership()
         */
        @AttributeDefinition(
                name = "Group auto membership",
                description = "List of groups that a synced group is added to automatically",
                cardinality = Integer.MAX_VALUE
        )
        String[] group_autoMembership();

        /**
         * @see DefaultSyncConfig.Group#getPropertyMapping()
         */
        @AttributeDefinition(
                name = "Group property mapping",
                description = "List mapping definition of local properties from external ones.",
                cardinality = Integer.MAX_VALUE
        )
        String[] group_propertyMapping();

        /**
         * @see DefaultSyncConfig.Group#getPathPrefix()
         */
        @AttributeDefinition(
                name = "Group Path Prefix",
                description = "The path prefix used when creating new groups."
        )
        String group_pathPrefix() default PARAM_GROUP_PATH_PREFIX_DEFAULT;

        /**
         * @see DefaultSyncConfig.Group#getDynamicGroups()
         */
        @AttributeDefinition(
                name = "Dynamic Groups",
                description = "If enabled external identity groups are synchronized as dynamic groups i.e. members/membership " +
                        "is resolved dynamically by a DynamicMembershipProvider. Note: currently this option only takes effect " +
                        "if 'User Dynamic Membership' is enabled."
        )
        boolean group_dynamicGroups() default PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT;

        /**
         * @see Authorizable#isApplyRFC7613UsernameCaseMapped()
         */
        @AttributeDefinition(
                name = "RFC7613 Username Normalization Profile",
                description = "Enable the UsercaseMappedProfile defined in RFC7613 for username normalization."
        )
        boolean enableRFC7613UsercaseMappedProfile() default PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT;
    }

    /**
     * Creates a new LDAP provider configuration based on the properties store in the given parameters.
     * @param params the configuration parameters.
     * @return the config
     */
    public static DefaultSyncConfig of(ConfigurationParameters params) {
        DefaultSyncConfig cfg = new DefaultSyncConfigImpl()
                .setName(params.getConfigValue(PARAM_NAME, PARAM_NAME_DEFAULT));

        cfg.user()
                .setDisableMissing(params.getConfigValue(PARAM_DISABLE_MISSING_USERS, PARAM_DISABLE_MISSING_USERS_DEFAULT))
                .setMembershipExpirationTime(getMilliSeconds(params, PARAM_USER_MEMBERSHIP_EXPIRATION_TIME, PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT, ONE_HOUR))
                .setMembershipNestingDepth(params.getConfigValue(PARAM_USER_MEMBERSHIP_NESTING_DEPTH, PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT))
                .setDynamicMembership(params.getConfigValue(PARAM_USER_DYNAMIC_MEMBERSHIP, PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT))
                .setEnforceDynamicMembership(params.getConfigValue(PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP, PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP_DEFAULT))
                .setExpirationTime(getMilliSeconds(params, PARAM_USER_EXPIRATION_TIME, PARAM_USER_EXPIRATION_TIME_DEFAULT, ONE_HOUR))
                .setApplyRFC7613UsernameCaseMapped(params.getConfigValue(PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT))
                .setPathPrefix(params.getConfigValue(PARAM_USER_PATH_PREFIX, PARAM_USER_PATH_PREFIX_DEFAULT))
                .setAutoMembership(params.getConfigValue(PARAM_USER_AUTO_MEMBERSHIP, PARAM_USER_AUTO_MEMBERSHIP_DEFAULT))
                .setPropertyMapping(createMapping(
                        params.getConfigValue(PARAM_USER_PROPERTY_MAPPING, PARAM_USER_PROPERTY_MAPPING_DEFAULT)));

        cfg.group()
                .setDynamicGroups(params.getConfigValue(PARAM_GROUP_DYNAMIC_GROUPS, PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT))
                .setExpirationTime(getMilliSeconds(params, PARAM_GROUP_EXPIRATION_TIME, PARAM_GROUP_EXPIRATION_TIME_DEFAULT, ONE_DAY))
                .setApplyRFC7613UsernameCaseMapped(params.getConfigValue(PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT))
                .setPathPrefix(params.getConfigValue(PARAM_GROUP_PATH_PREFIX, PARAM_GROUP_PATH_PREFIX_DEFAULT))
                .setAutoMembership(params.getConfigValue(PARAM_GROUP_AUTO_MEMBERSHIP, PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT))
                .setPropertyMapping(createMapping(
                        params.getConfigValue(PARAM_GROUP_PROPERTY_MAPPING, PARAM_GROUP_PROPERTY_MAPPING_DEFAULT)));

        return cfg;
    }

    private static long getMilliSeconds(@NotNull ConfigurationParameters params, @NotNull String paramName,
                                        @NotNull String defaultParamValue,
                                        @NotNull ConfigurationParameters.Milliseconds defaultMillis) {
        return ConfigurationParameters.Milliseconds.of(params.getConfigValue(paramName, defaultParamValue), defaultMillis).value;
    }

    /**
     * Creates a new property mapping map from a list of patterns.
     * @param patterns the patterns
     * @return the mapping map
     */
    private static Map<String, String> createMapping(@NotNull String[] patterns) {
        Map<String, String> mapping = new HashMap<>();
        for (String pattern: patterns) {
            int idx = pattern.indexOf('=');
            if (idx > 0) {
                String relPath = pattern.substring(0, idx).trim();
                String value = pattern.substring(idx+1).trim();
                mapping.put(relPath, value);
            }
        }
        return mapping;
    }

}
