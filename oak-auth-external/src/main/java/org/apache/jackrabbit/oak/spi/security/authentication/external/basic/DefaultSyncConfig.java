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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * {@code DefaultSyncConfig} defines how users and groups from an external source are synced into the repository using
 * the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler}.
 */
public class DefaultSyncConfig {

    private final User user = new User();

    private final Group group = new Group();

    private String name = "default";

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
    @Nonnull
    public User user() {
        return user;
    }

    /**
     * Returns the sync configuration for groups.
     * @return the group sync configuration.
     */
    @Nonnull
    public Group group() {
        return group;
    }

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
            return autoMembership == null ? Collections.<String>emptySet() : autoMembership;
        }

        /**
         * Sets the auto membership
         * @param autoMembership the membership
         * @return {@code this}
         * @see #getAutoMembership()
         */
        @Nonnull
        public Authorizable setAutoMembership(String ... autoMembership) {
            this.autoMembership = new HashSet<String>();
            for (String groupName: autoMembership) {
                if (!groupName.trim().isEmpty()) {
                    this.autoMembership.add(groupName.trim());
                }
            }
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
            return propertyMapping == null ? Collections.<String, String>emptyMap() : propertyMapping;
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
            return pathPrefix == null ? "" : pathPrefix;
        }

        /**
         * Sets the path prefix.
         * @param pathPrefix the path prefix.
         * @return {@code this}
         * @see #getPathPrefix()
         */
        @Nonnull
        public Authorizable setPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
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
}