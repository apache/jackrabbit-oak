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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

/**
 * {@code DefaultSyncConfig} defines how users and groups from an external source are synced into the repository using
 * the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler}.
 */
public class DefaultSyncConfig {
    
    public static final String DEFAULT_NAME = "default";

    private final User user = new User();

    private final Group group = new Group();

    private String name = DEFAULT_NAME;

    /**
     * Configures the name of this configuration
     * @return the name
     */
    @NotNull
    public String getName() {
        return name;
    }

    /**
     * Sets the name
     * @param name the name
     * @return {@code this}
     * @see #getName()
     */
    @NotNull
    public DefaultSyncConfig setName(@NotNull String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the sync configuration for users.
     * @return the user sync configuration.
     */
    @NotNull
    public User user() {
        return user;
    }

    /**
     * Returns the sync configuration for groups.
     * @return the group sync configuration.
     */
    @NotNull
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

        private boolean applyRFC7613UsernameCaseMapped;

        private AutoMembershipConfig autoMembershipConfig = AutoMembershipConfig.EMPTY;

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
        @NotNull
        public Authorizable setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        /**
         * Returns true if new AuthorizableIDs will be normalized according to
         * the UsernameCaseMapped profile defined in RFC7613
         * @return true if new AuthorizableIDs will be normalized.
         */
        public boolean isApplyRFC7613UsernameCaseMapped() {
            return applyRFC7613UsernameCaseMapped;
        }

        /**
         * Set to true if new AuthorizableIDs shall be normalized according to
         * the UsernameCaseMapped profile defined in RFC7613.
         * @param applyRFC7613UsernameCaseMapped true if the UsernameCaseMapped profile shall be used for normalization.
         * @return {@code this}
         * @see #isApplyRFC7613UsernameCaseMapped()
         */
        public Authorizable setApplyRFC7613UsernameCaseMapped(boolean applyRFC7613UsernameCaseMapped) {
            this.applyRFC7613UsernameCaseMapped = applyRFC7613UsernameCaseMapped;
            return this;
        }

        /**
         * Returns the values configured with {@link #setAutoMembership(String...)}. 
         * 
         * Note that this method only takes group names into account that have been configured using {@link #setAutoMembership(String...)}.
         * In order to take advantage of the conditional auto-membership as defined with {@link #setAutoMembershipConfig(AutoMembershipConfig)},
         * use {@link #getAutoMembership(org.apache.jackrabbit.api.security.user.Authorizable)} instead.
         * 
         * @return set of group names as defined with {@link #setAutoMembership(String...)}
         */
        @NotNull
        public Set<String> getAutoMembership() {
            return autoMembership == null ? Collections.emptySet() : autoMembership;
        }

        /**
         * Sets the auto membership. Note that the passed group names will be trimmed
         * and empty string values will be ignored (along with {@code null} values).
         *
         * @param autoMembership the membership
         * @return {@code this}
         * @see #getAutoMembership()
         */
        @NotNull
        public Authorizable setAutoMembership(@NotNull String ... autoMembership) {
            this.autoMembership = new HashSet<>();
            for (String groupName: autoMembership) {
                if (groupName != null && !groupName.trim().isEmpty()) {
                    this.autoMembership.add(groupName.trim());
                }
            }
            return this;
        }

        /**
         * Gets the {@code AutoMembershipConfig} that applies to this configuration.
         * 
         * @return {@code this}
         * @see #setAutoMembershipConfig(AutoMembershipConfig) 
         * @see #getAutoMembership(org.apache.jackrabbit.api.security.user.Authorizable) 
         */
        @NotNull
        public AutoMembershipConfig getAutoMembershipConfig() {
            return autoMembershipConfig;
        }

        /**
         * Sets the {@code AutoMembershipConfiguration} that applies to this configuration.
         *
         * @return {@code this}
         * @see #getAutoMembershipConfig()
         * @see #getAutoMembership(org.apache.jackrabbit.api.security.user.Authorizable)
         */
        @NotNull
        public Authorizable setAutoMembershipConfig(@NotNull AutoMembershipConfig autoMembershipConfig) {
            this.autoMembershipConfig = autoMembershipConfig;
            return this;
        }
        
        /**
         * Defines the set of group ids a synchronized external user/group is automatically member of. In contrast to
         * {@link #getAutoMembership()} this method respects both values configured with {@link #setAutoMembership(String...)} 
         * and with {@link #setAutoMembershipConfig(AutoMembershipConfig)}.
         * 
         * @return set of group ids.
         */
        @NotNull
        public Set<String> getAutoMembership(@NotNull org.apache.jackrabbit.api.security.user.Authorizable authorizable) {
            return Stream.concat(autoMembershipConfig.getAutoMembership(authorizable).stream().filter(Objects::nonNull), getAutoMembership().stream().filter(Objects::nonNull)).collect(Collectors.toUnmodifiableSet());
        }

        /**
         * Defines the mapping of internal property names from external values. Only the external properties defined as
         * keys of this map are synced with the mapped internal properties. note that the property names can be relative
         * paths. the intermediate nodes will be created accordingly.
         *
         * Example:
         * <pre>{@code
         *     {
         *         "rep:fullname": "cn",
         *         "country", "c",
         *         "profile/email": "mail",
         *         "profile/givenName": "cn"
         *     }
         * }</pre>
         *
         * The implicit properties like userid, groupname, password must not be mapped.
         *
         * @return the property mapping where the keys are the local property names and the values the external ones.
         */
        @NotNull
        public Map<String, String> getPropertyMapping() {
            return propertyMapping == null ? Collections.emptyMap() : propertyMapping;
        }

        /**
         * Sets the property mapping.
         * @param propertyMapping the mapping
         * @return {@code this}
         * @see #getPropertyMapping()
         */
        @NotNull
        public Authorizable setPropertyMapping(@NotNull Map<String, String> propertyMapping) {
            this.propertyMapping = propertyMapping;
            return this;
        }

        /**
         * Defines the authorizables intermediate path prefix that is used when creating new authorizables. This prefix
         * is always prepended to the path provided by the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity}.
         * @return the intermediate path prefix.
         */
        @NotNull
        public String getPathPrefix() {
            return pathPrefix == null ? "" : pathPrefix;
        }

        /**
         * Sets the path prefix.
         * @param pathPrefix the path prefix.
         * @return {@code this}
         * @see #getPathPrefix()
         */
        @NotNull
        public Authorizable setPathPrefix(@NotNull String pathPrefix) {
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

        private boolean dynamicMembership;
        
        private boolean enforceDynamicMembership;

        private boolean disableMissing;

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
        @NotNull
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
        @NotNull
        public User setMembershipNestingDepth(long membershipNestingDepth) {
            this.membershipNestingDepth = membershipNestingDepth;
            return this;
        }

        /**
         * Returns {@code true} if a dynamic group membership is enabled.
         *
         * Turning this option on may alter the behavior of other configuration
         * options dealing with synchronization of group accounts and group membership.
         * In particular it's an implementation detail if external groups may
         * no longer be synchronized into the repository.
         *
         * @return {@code true} if dynamic group membership for external
         * user identities is turn on; {@code false} otherwise.
         */
        public boolean getDynamicMembership() {
            return dynamicMembership;
        }

        /**
         * Enable or disable the dynamic group membership. If turned on external
         * identities and their group membership will be synchronized such that the
         * membership information is generated dynamically. External groups may
         * or may not be synchronized into the repository if this option is turned
         * on.
         *
         * @param dynamicMembership Boolean flag to enable or disable a dedicated
         *                      dynamic group management.
         * @return {@code this}
         * @see #getDynamicMembership() for details.
         */
        @NotNull
        public User setDynamicMembership(boolean dynamicMembership) {
            this.dynamicMembership = dynamicMembership;
            return this;
        }

        /**
         * Returns {@code true} if a dynamic group membership must be enforced for users that have been synchronized
         * previously. Note that this option has no effect if {@link #getDynamicMembership()} returns {@code false}.
         *
         * @return {@code true} if dynamic group membership for external user identities must be enforced for previously 
         * synced users; {@code false} otherwise. This option only takes effect if {@link #getDynamicMembership()} is enabled.
         */
        public boolean getEnforceDynamicMembership() {
            return enforceDynamicMembership;
        }

        /**
         * Enable or disable the enforcement of dynamic group membership.
         *
         * @param enforceDynamicMembership Boolean flag to define if dynamic group management is enforced for previously synced users. 
         * @return {@code this}
         * @see #getEnforceDynamicMembership() () for details.
         */
        public User setEnforceDynamicMembership(boolean enforceDynamicMembership) {
            this.enforceDynamicMembership = enforceDynamicMembership;
            return this;
        }
        
        /**
         * Controls the behavior for users that no longer exist on the external provider. The default is to delete the repository users
         * if they no longer exist on the external provider. If set to true, they will be disabled instead, and re-enabled once they appear
         * again.
         */
        public boolean getDisableMissing() {
            return disableMissing;
        }

        /**
         * @see #getDisableMissing()
         */
        public User setDisableMissing(boolean disableMissing) {
            this.disableMissing = disableMissing;
            return this;
        }
    }

    /**
     * Group specific config
     */
    public static class Group extends Authorizable {
        
        private boolean dynamicGroups;

        /**
         * <p>Returns {@code true} if external group identities are being synchronized into the repository as dynamic groups.
         * In this case a dedicated {@link org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider} must be 
         * present in order to have group membership reflected through User Management API.</p>
         * 
         * <p>Note, that currently this option only takes effect if it is enabled together with dynamic membership 
         * (i.e. {@link User#getDynamicMembership()} returns true). In this case a dedicated 
         * {@link org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider} based on the 
         * {@code ExternalGroupPrincipalProvider} will be registered.</p>
         *
         * @return {@code true} if external groups should be synchronized as dynamic groups (i.e. without having their
         * members added); {@code false} otherwise. Note, that this option currently only takes effect if {@link User#getDynamicMembership()} is enabled.
         */
        public boolean getDynamicGroups() {
            return dynamicGroups;
        }

        /**
         * Enable or disable the dynamic group option. If turned on together with {@link User#getDynamicMembership()} 
         * external group identities will be synchronized into the repository but without storing their members. 
         * In other words, group membership is generated dynamically.
         *
         * @param dynamicGroups Boolean flag to enable or disable synchronization of dynamic groups.
         * @return {@code this}
         * @see #getDynamicGroups() for details.
         */
        public @NotNull Group setDynamicGroups(boolean dynamicGroups) {
            this.dynamicGroups = dynamicGroups;
            return this;
        }
    }
}
