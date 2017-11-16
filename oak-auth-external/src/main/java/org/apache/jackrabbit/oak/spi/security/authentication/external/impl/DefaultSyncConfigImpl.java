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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.osgi.framework.ServiceReference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * {@code DefaultSyncConfig} defines how users and groups from an external source are synced into the repository using
 * the {@link DefaultSyncHandler}.
 */
public class DefaultSyncConfigImpl extends DefaultSyncConfig {

    public static final String PARAM_NAME = "handler.name";
    public static final String PARAM_USER_DYNAMIC_MEMBERSHIP = "user.dynamicMembership";
    public static final String PARAM_USER_AUTO_MEMBERSHIP = "user.autoMembership";
    public static final String PARAM_GROUP_AUTO_MEMBERSHIP = "group.autoMembership";
    public static final boolean PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT = false;
    public static final String PARAM_NAME_DEFAULT = "default";
    
    
	/**
     * Creates a new LDAP provider configuration based on the properties store in the given parameters.
     * @param params the configuration parameters.
     * @return the config
     */
    public static DefaultSyncConfig of(SyncConfig config) {
        DefaultSyncConfig cfg = new DefaultSyncConfigImpl()
                .setName(config.handler_name());

        cfg.user()
                .setDisableMissing(config.user_disableMissing())
                .setMembershipExpirationTime(getMilliSeconds(config.user_membershipExpTime()))
                .setMembershipNestingDepth(config.user_membershipNestingDepth())
                .setDynamicMembership(config.user_dynamicMembership())
                .setExpirationTime(getMilliSeconds(config.user_expirationTime()))
                .setApplyRFC7613UsernameCaseMapped(config.enableRFC7613UsercaseMappedProfile())
                .setPathPrefix(config.user_pathPrefix())
                .setAutoMembership(config.user_autoMembership())
                .setPropertyMapping(createMapping(config.user_propertyMapping()));

        cfg.group()
                .setExpirationTime(getMilliSeconds(config.group_expirationTime()))
                .setApplyRFC7613UsernameCaseMapped(config.enableRFC7613UsercaseMappedProfile())
                .setPathPrefix(config.group_pathPrefix())
                .setAutoMembership(config.group_autoMembership())
                .setPropertyMapping(createMapping(config.group_propertyMapping()));

        return cfg;
    }

	private static long getMilliSeconds(String ms) {
        return ConfigurationParameters.Milliseconds.of(ms).value;
    }

    /**
     * Creates a new property mapping map from a list of patterns.
     * @param patterns the patterns
     * @return the mapping map
     */
    private static Map<String, String> createMapping(@Nonnull String[] patterns) {
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
    
    public static String getSyncHandlerName(ServiceReference ref) {
        return PropertiesUtil.toString(ref.getProperty("handler.name"), "default");
    }

    public static String[] getAutoMembership(ServiceReference ref) {
        return PropertiesUtil.toStringArray(ref.getProperty("user.autoMembership"), new String[0]);
    }
    
    public static boolean hasDynamicMembership(ServiceReference reference) {
        return PropertiesUtil.toBoolean(reference.getProperty(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP), false);
    }

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak Default Sync Handler"
    )
    @interface SyncConfig {

        @AttributeDefinition(name = "Sync Handler Name", 
                description = "Name of this sync configuration. This is used to reference this handler by the login modules.")
        String handler_name() default "default";
        
        @AttributeDefinition(name = "User Expiration Time",
                description = "Duration until a synced user gets expired (eg. '1h 30m' or '1d').")
        String user_expirationTime() default "1h";

        @AttributeDefinition(name = "User auto membership",
                description = "List of groups that a synced user is added to automatically")
        String[] user_autoMembership() default {};

        @AttributeDefinition(name = "User property mapping",
                description = "List mapping definition of local properties from external ones. eg: 'profile/email=mail'." +
                        "Use double quotes for fixed values. eg: 'profile/nt:primaryType=\"nt:unstructured\"")
        String[] user_propertyMapping() default {"rep:fullname=cn"};
        
        @AttributeDefinition(name = "User Path Prefix",
                description = "The path prefix used when creating new users.")
        String user_pathPrefix() default "";
        
        @AttributeDefinition(name = "User Membership Expiration",
                description = "Time after which membership expires (eg. '1h 30m' or '1d'). Note however, that a membership sync is aways bound to a sync of the user.")
        String user_membershipExpTime() default "1h";

        @AttributeDefinition(name = "User membership nesting depth",
                description = "Returns the maximum depth of group nesting when membership relations are synced. " +
                        "A value of 0 effectively disables group membership lookup. A value of 1 only adds the direct " +
                        "groups of a user. This value has no effect when syncing individual groups only when syncing a " +
                        "users membership ancestry.")
        int user_membershipNestingDepth() default 0;

        
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
        @AttributeDefinition(name = "User Dynamic Membership",
                description = "If enabled membership of external identities (user) is no longer fully reflected " +
                        "within the repositories user management.")
        boolean user_dynamicMembership() default false;

        @AttributeDefinition(name = "Disable missing users",
                description = "If true, users that no longer exist on the external provider will be locally disabled, " +
                        "and re-enabled if they become valid again. If false (default) they will be removed.")
        boolean user_disableMissing() default false;

        @AttributeDefinition(name = "Group Expiration Time",
                description = "Duration until a synced group expires (eg. '1h 30m' or '1d').")
        String group_expirationTime() default "1d";

        @AttributeDefinition(name = "Group auto membership",
                description = "List of groups that a synced group is added to automatically")
        String group_autoMembership() default "";

        @AttributeDefinition(name = "Group property mapping",
                description = "List mapping definition of local properties from external ones.")
        String[] group_propertyMapping() default {};

        @AttributeDefinition(name = "Group Path Prefix",
                description = "The path prefix used when creating new groups")
        String group_pathPrefix() default "";

        @AttributeDefinition(name = "RFC7613 Username Normalization Profile",
                description = "Enable the UsercaseMappedProfile defined in RFC7613 for username normalization.")
        boolean enableRFC7613UsercaseMappedProfile() default false;

    }
}