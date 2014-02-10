/*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright ${today.year} Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Map;
import java.util.Set;

/**
 * {@code SyncConfig} defines how users and groups from an external source are synced into the repository.
 */
public class SyncConfig {

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

        public void setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        /**
         * Defines the set of group names that are automatically added to synced authorizable.
         * @return set of group names.
         */
        public Set<String> getAutoMembership() {
            return autoMembership;
        }

        public void setAutoMembership(Set<String> autoMembership) {
            this.autoMembership = autoMembership;
        }

        /**
         * Defines the mapping from external to internal property names. Only the external properties defined as keys of
         * this map are synced with the mapped internal properties.
         *
         * Example:
         * <xmp>
         *     {
         *         "cn": "givenName",
         *         "c": "country"
         *     }
         * </xmp>
         *
         * The implicit properties like userid, groupname, password must not be mapped.
         *
         * @return the property mapping
         */
        public Map<String, String> getPropertyMapping() {
            return propertyMapping;
        }

        public void setPropertyMapping(Map<String, String> propertyMapping) {
            this.propertyMapping = propertyMapping;
        }

        /**
         * Defines the authorizables intermediate path prefix that is used when creating new authorizables. This prefix
         * is always prepended to the path provided by the {@link ExternalIdentity}.
         * @return the intermediate path prefix.
         */
        public String getPathPrefix() {
            return pathPrefix;
        }

        public void setPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }
    }

    /**
     * User specific config.
     */
    public static class User extends Authorizable {

        private long membershipExpirationTime;

        private long groupNestingDepth;

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

        public void setMembershipExpirationTime(long membershipExpirationTime) {
            this.membershipExpirationTime = membershipExpirationTime;
        }

        /**
         * Returns the maximum depth of group nesting when membership relations are synced. A value of 0 effectively
         * disables group membership lookup. A value of 1 only adds the direct groups of a user. This value has no effect
         * when syncing individual groups only when syncing a users membership ancestry.
         * @return the group nesting depth
         */
        public long getGroupNestingDepth() {
            return groupNestingDepth;
        }

        public void setGroupNestingDepth(long groupNestingDepth) {
            this.groupNestingDepth = groupNestingDepth;
        }

    }

    /**
     * Group specific config
     */
    public static class Group extends Authorizable {

    }

    private final User user = new User();

    private final Group group = new Group();

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