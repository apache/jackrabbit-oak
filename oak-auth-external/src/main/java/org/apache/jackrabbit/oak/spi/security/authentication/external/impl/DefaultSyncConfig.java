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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

/**
 * {@code DefaultSyncConfig} defines how users and groups from an external source are synced into the repository using
 * the {@link DefaultSyncHandler}.
 */
@Component(
        label = "Apache Jackrabbit Oak Default Sync Handler",
        name = "org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler",
        configurationFactory = true,
        metatype = true,
        ds = false
)
public class DefaultSyncConfig {

    /**
     * @see #getName()
     */
    public static final String PARAM_NAME_DEFAULT = "default";

    /**
     * @see #getName()
     */
    @Property(
            label = "Sync Handler Name",
            description = "Name of this sync configuration. This is used to reference this handler by the login modules.",
            value = PARAM_NAME_DEFAULT
    )
    public static final String PARAM_NAME = "handler.name";

    /**
     * @see DefaultSyncConfig.User#getExpirationTime()
     */
    public static final String PARAM_USER_EXPIRATION_TIME_DEFAULT = "1h";

    /**
     * @see DefaultSyncConfig.User#getExpirationTime()
     */
    @Property(
            label = "User Expiration Time",
            description = "Duration until a synced user gets expired (eg. '1h 30m' or '1d').",
            value = PARAM_USER_EXPIRATION_TIME_DEFAULT
    )
    public static final String PARAM_USER_EXPIRATION_TIME = "user.expirationTime";

    /**
     * @see DefaultSyncConfig.User#getAutoMembership()
     */
    public static final String[] PARAM_USER_AUTO_MEMBERSHIP_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.User#getAutoMembership()
     */
    @Property(
            label = "User auto membership",
            description = "List of groups that a synced user is added to automatically",
            value = {},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_USER_AUTO_MEMBERSHIP = "user.autoMembership";

    /**
     * @see DefaultSyncConfig.User#getPropertyMapping()
     */
    public static final String[] PARAM_USER_PROPERTY_MAPPING_DEFAULT = {"rep:fullname=cn"};

    /**
     * @see DefaultSyncConfig.User#getPropertyMapping()
     */
    @Property(
            label = "User property mapping",
            description = "List mapping definition of local properties from external ones. eg: 'profile/email=mail'." +
                    "Use double quotes for fixed values. eg: 'profile/nt:primaryType=\"nt:unstructured\"",
            value = {"rep:fullname=cn"},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_USER_PROPERTY_MAPPING = "user.propertyMapping";

    /**
     * @see DefaultSyncConfig.User#getPathPrefix()
     */
    public static final String PARAM_USER_PATH_PREFIX_DEFAULT = "";

    /**
     * @see DefaultSyncConfig.User#getPathPrefix()
     */
    @Property(
            label = "User Path Prefix",
            description = "The path prefix used when creating new users.",
            value = PARAM_USER_PATH_PREFIX_DEFAULT
    )
    public static final String PARAM_USER_PATH_PREFIX = "user.pathPrefix";

    /**
     * @see DefaultSyncConfig.User#getMembershipExpirationTime()
     */
    public static final String PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT = "1h";

    /**
     * @see DefaultSyncConfig.User#getMembershipExpirationTime()
     */
    @Property(
            label = "User Membership Expiration",
            description = "Time after which membership expires (eg. '1h 30m' or '1d').",
            value = PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT
    )
    public static final String PARAM_USER_MEMBERSHIP_EXPIRATION_TIME = "user.membershipExpTime";

    /**
     * @see User#getMembershipNestingDepth()
     */
    public static final int PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT = 0;

    /**
     * @see User#getMembershipNestingDepth()
     */
    @Property(
            label = "User membership nesting depth",
            description = "Returns the maximum depth of group nesting when membership relations are synced. " +
                    "A value of 0 effectively disables group membership lookup. A value of 1 only adds the direct " +
                    "groups of a user. This value has no effect when syncing individual groups only when syncing a " +
                    "users membership ancestry.",
            intValue = PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT
    )
    public static final String PARAM_USER_MEMBERSHIP_NESTING_DEPTH = "user.membershipNestingDepth";

    /**
     * @see DefaultSyncConfig.Group#getExpirationTime()
     */
    public static final String PARAM_GROUP_EXPIRATION_TIME_DEFAULT = "1d";

    /**
     * @see DefaultSyncConfig.Group#getExpirationTime()
     */
    @Property(
            label = "Group Expiration Time",
            description = "Duration until a synced group expires (eg. '1h 30m' or '1d').",
            value = PARAM_GROUP_EXPIRATION_TIME_DEFAULT
    )
    public static final String PARAM_GROUP_EXPIRATION_TIME = "group.expirationTime";

    /**
     * @see DefaultSyncConfig.Group#getAutoMembership()
     */
    public static final String[] PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.Group#getAutoMembership()
     */
    @Property(
            label = "Group auto membership",
            description = "List of groups that a synced group is added to automatically",
            value = {},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_GROUP_AUTO_MEMBERSHIP = "group.autoMembership";

    /**
     * @see DefaultSyncConfig.Group#getPropertyMapping()
     */
    public static final String[] PARAM_GROUP_PROPERTY_MAPPING_DEFAULT = {};

    /**
     * @see DefaultSyncConfig.Group#getPropertyMapping()
     */
    @Property(
            label = "Group property mapping",
            description = "List mapping definition of local properties from external ones.",
            value = {},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_GROUP_PROPERTY_MAPPING = "group.propertyMapping";

    /**
     * @see DefaultSyncConfig.Group#getPathPrefix()
     */
    public static final String PARAM_GROUP_PATH_PREFIX_DEFAULT = "";

    /**
     * @see DefaultSyncConfig.Group#getPathPrefix()
     */
    @Property(
            label = "Group Path Prefix",
            description = "The path prefix used when creating new groups.",
            value = PARAM_GROUP_PATH_PREFIX_DEFAULT
    )
    public static final String PARAM_GROUP_PATH_PREFIX = "group.pathPrefix";

    /**
     * Base config class for users and groups
     */
    public abstract static class Authorizable {

        private long expirationTime;

        private Set<String> autoMembership;

        private Map<String, String> propertyMapping;

        private String pathPrefix;

        /**
         * Returns the duration in milliseconds until a synced authorizable gets expired. An expired authorizable will
         * be re-synced.
         * @return the expiration time in milliseconds.
         */
        public long getExpirationTime() {
            return expirationTime;
        }

        /**
         * Sets the expiration time.
         * @param expirationTime time in milliseconds.
         * @return {@code this}
         * @see #getExpirationTime()
         */
        @Nonnull
        public Authorizable setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        /**
         * Defines the set of group names that are automatically added to synced authorizable.
         * @return set of group names.
         */
        @Nonnull
        public Set<String> getAutoMembership() {
            return autoMembership;
        }

        /**
         * Sets the auto membership
         * @param autoMembership the membership
         * @return {@code this}
         * @see #getAutoMembership()
         */
        @Nonnull
        public Authorizable setAutoMembership(String ... autoMembership) {
            this.autoMembership = new HashSet<String>(Arrays.asList(autoMembership));
            return this;
        }

        /**
         * Defines the mapping of internal property names from external values. Only the external properties defined as
         * keys of this map are synced with the mapped internal properties. note that the property names can be relative
         * paths. the intermediate nodes will be created accordingly.
         *
         * Example:
         * <xmp>
         *     {
         *         "rep:fullname": "cn",
         *         "country", "c",
         *         "profile/email": "mail",
         *         "profile/givenName": "cn"
         *     }
         * </xmp>
         *
         * The implicit properties like userid, groupname, password must not be mapped.
         *
         * @return the property mapping where the keys are the local property names and the values the external ones.
         */
        @Nonnull
        public Map<String, String> getPropertyMapping() {
            return propertyMapping;
        }

        /**
         * Sets the property mapping.
         * @param propertyMapping the mapping
         * @return {@code this}
         * @see #getPropertyMapping()
         */
        @Nonnull
        public Authorizable setPropertyMapping(Map<String, String> propertyMapping) {
            this.propertyMapping = propertyMapping;
            return this;
        }

        /**
         * Defines the authorizables intermediate path prefix that is used when creating new authorizables. This prefix
         * is always prepended to the path provided by the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity}.
         * @return the intermediate path prefix.
         */
        @Nonnull
        public String getPathPrefix() {
            return pathPrefix;
        }

        /**
         * Sets the path prefix.
         * @param pathPrefix the path prefix.
         * @return {@code this}
         * @see #getPathPrefix()
         */
        @Nonnull
        public Authorizable setPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix == null ? "" : pathPrefix;
            return this;
        }
    }

    /**
     * User specific config.
     */
    public static class User extends Authorizable {

        private long membershipExpirationTime;

        private long membershipNestingDepth;

        /**
         * Returns the duration in milliseconds until the group membership of a user is expired. If the
         * membership information is expired it is re-synced according to the maximum nesting depth.
         * Note that the membership is the groups an authorizable is member of, not the list of members of a group.
         * Also note, that the group membership expiration time can be higher than the user expiration time itself and
         * that value has no effect when syncing individual groups only when syncing a users membership ancestry.
         *
         * @return the expiration time in milliseconds.
         */
        public long getMembershipExpirationTime() {
            return membershipExpirationTime;
        }

        /**
         * Sets the membership expiration time
         * @param membershipExpirationTime the time in milliseconds.
         * @return {@code this}
         * @see #getMembershipExpirationTime()
         */
        @Nonnull
        public User setMembershipExpirationTime(long membershipExpirationTime) {
            this.membershipExpirationTime = membershipExpirationTime;
            return this;
        }

        /**
         * Returns the maximum depth of group nesting when membership relations are synced. A value of 0 effectively
         * disables group membership lookup. A value of 1 only adds the direct groups of a user. This value has no effect
         * when syncing individual groups only when syncing a users membership ancestry.
         * @return the group nesting depth
         */
        public long getMembershipNestingDepth() {
            return membershipNestingDepth;
        }

        /**
         * Sets the group nesting depth.
         * @param membershipNestingDepth the depth.
         * @return {@code this}
         * @see #getMembershipNestingDepth()
         */
        @Nonnull
        public User setMembershipNestingDepth(long membershipNestingDepth) {
            this.membershipNestingDepth = membershipNestingDepth;
            return this;
        }

    }

    /**
     * Group specific config
     */
    public static class Group extends Authorizable {

    }

    /**
     * Creates a new LDAP provider configuration based on the properties store in the given parameters.
     * @param params the configuration parameters.
     * @return the config
     */
    public static DefaultSyncConfig of(ConfigurationParameters params) {
        DefaultSyncConfig cfg = new DefaultSyncConfig()
                .setName(params.getConfigValue(PARAM_NAME, PARAM_NAME_DEFAULT));

        cfg.user()
                .setMembershipExpirationTime(
                        ConfigurationParameters.Milliseconds.of(params.getConfigValue(PARAM_USER_MEMBERSHIP_EXPIRATION_TIME, PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT)).value
                )
                .setMembershipNestingDepth(params.getConfigValue(PARAM_USER_MEMBERSHIP_NESTING_DEPTH, PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT))
                .setExpirationTime(
                        ConfigurationParameters.Milliseconds.of(params.getConfigValue(PARAM_USER_EXPIRATION_TIME, PARAM_USER_EXPIRATION_TIME_DEFAULT)).value
                )
                .setPathPrefix(params.getConfigValue(PARAM_USER_PATH_PREFIX, PARAM_USER_PATH_PREFIX_DEFAULT))
                .setAutoMembership(params.getConfigValue(PARAM_USER_AUTO_MEMBERSHIP, PARAM_USER_AUTO_MEMBERSHIP_DEFAULT))
                .setPropertyMapping(createMapping(
                        params.getConfigValue(PARAM_USER_PROPERTY_MAPPING, PARAM_USER_PROPERTY_MAPPING_DEFAULT)));

        cfg.group()
                .setExpirationTime(
                        ConfigurationParameters.Milliseconds.of(params.getConfigValue(PARAM_GROUP_EXPIRATION_TIME, PARAM_GROUP_EXPIRATION_TIME_DEFAULT)).value
                )
                .setPathPrefix(params.getConfigValue(PARAM_GROUP_PATH_PREFIX, PARAM_GROUP_PATH_PREFIX_DEFAULT))
                .setAutoMembership(params.getConfigValue(PARAM_GROUP_AUTO_MEMBERSHIP, PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT))
                .setPropertyMapping(createMapping(
                        params.getConfigValue(PARAM_GROUP_PROPERTY_MAPPING, PARAM_GROUP_PROPERTY_MAPPING_DEFAULT)));

        return cfg;
    }

    /**
     * Creates a new property mapping map from a list of patterns.
     * @param patterns the patterns
     * @return the mapping map
     */
    private static Map<String, String> createMapping(String[] patterns) {
        Map<String, String> mapping = new HashMap<String, String>();
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

    private String name = PARAM_NAME_DEFAULT;

    private final User user = new User();

    private final Group group = new Group();

    /**
     * Configures the name of this configuration
     * @return the name
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Sets the name
     * @param name the name
     * @return {@code this}
     * @see #getName()
     */
    public DefaultSyncConfig setName(@Nonnull String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the sync configuration for users.
     * @return the user sync configuration.
     */
    public User user() {
        return user;
    }

    /**
     * Returns the sync configuration for groups.
     * @return the group sync configuration.
     */
    public Group group() {
        return group;
    }

}