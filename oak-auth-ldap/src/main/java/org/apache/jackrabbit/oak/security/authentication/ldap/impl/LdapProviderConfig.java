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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

/**
 * Configuration of the ldap provider.
 */
@Component(
        label = "Apache Jackrabbit Oak LDAP Identity Provider",
        name = "org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapIdentityProvider",
        configurationFactory = true,
        metatype = true,
        ds = false
)
public class LdapProviderConfig {

    /**
     * @see #getName()
     */
    public static final String PARAM_NAME_DEFAULT = "ldap";

    /**
     * @see #getName()
     */
    @Property(
            label = "LDAP Provider Name",
            description = "Name of this LDAP provider configuration. This is used to reference this provider by the login modules.",
            value = PARAM_NAME_DEFAULT
    )
    public static final String PARAM_NAME = "provider.name";

    /**
     * @see #getHostname()
     */
    public static final String PARAM_LDAP_HOST_DEFAULT = "localhost";

    /**
     * @see #getHostname()
     */
    @Property(
            label = "LDAP Server Hostname",
            description = "Hostname of the LDAP server",
            value = PARAM_LDAP_HOST_DEFAULT
    )
    public static final String PARAM_LDAP_HOST = "host.name";

    /**
     * @see #getPort()
     */
    public static final int PARAM_LDAP_PORT_DEFAULT = 389;

    /**
     * @see #getPort()
     */
    @Property(
            label = "LDAP Server Port",
            description = "Port of the LDAP server",
            intValue = PARAM_LDAP_PORT_DEFAULT
    )
    public static final String PARAM_LDAP_PORT = "host.port";

    /**
     * @see #useSSL()
     */
    public static final boolean PARAM_USE_SSL_DEFAULT = false;

    /**
     * @see #useSSL()
     */
    @Property(
            label = "Use SSL",
            description = "Indicates if an SSL connection should be used.",
            boolValue = PARAM_USE_SSL_DEFAULT
    )
    public static final String PARAM_USE_SSL = "host.ssl";

    /**
     * @see #getBindDN()
     */
    public static final String PARAM_BIND_DN_DEFAULT = "";

    /**
     * @see #getBindDN()
     */
    @Property(
            label = "Bind DN",
            description = "DN of the user for authentication. Leave empty for anonymous bind.",
            value = PARAM_BIND_DN_DEFAULT
    )
    public static final String PARAM_BIND_DN = "bind.dn";

    /**
     * @see #getBindPassword()
     */
    public static final String PARAM_BIND_PASSWORD_DEFAULT = "";

    /**
     * @see #getBindPassword()
     */
    @Property(
            label = "Bind Password",
            description = "Password of the user for authentication.",
            passwordValue = PARAM_BIND_PASSWORD_DEFAULT
    )
    public static final String PARAM_BIND_PASSWORD = "bind.password";

    /**
     * @see #getSearchTimeout()
     */
    public static final int PARAM_SEARCH_TIMEOUT_DEFAULT = 60000;

    /**
     * @see #getSearchTimeout()
     */
    @Property(
            label = "Search Timeout",
            description = "Time in milliseconds until a search times out.",
            intValue = PARAM_SEARCH_TIMEOUT_DEFAULT
    )
    public static final String PARAM_SEARCH_TIMEOUT = "searchTimeout";

    /**
     * @see Identity#getBaseDN()
     */
    public static final String PARAM_USER_BASE_DN_DEFAULT = "ou=people,o=example,dc=com";

    /**
     * @see Identity#getBaseDN()
     */
    @Property(
            label = "User base DN",
            description = "The base DN for user searches.",
            value = PARAM_USER_BASE_DN_DEFAULT
    )
    public static final String PARAM_USER_BASE_DN = "user.baseDN";

    /**
     * @see Identity#getObjectClasses()
     */
    public static final String[] PARAM_USER_OBJECTCLASS_DEFAULT = {"person"};

    /**
     * @see Identity#getObjectClasses()
     */
    @Property(
            label = "User object classes",
            description = "The list of object classes an user entry must contain.",
            value = {"person"},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_USER_OBJECTCLASS = "user.objectclass";

    /**
     * @see Identity#getIdAttribute()
     */
    public static final String PARAM_USER_ID_ATTRIBUTE_DEFAULT = "uid";

    /**
     * @see Identity#getIdAttribute()
     */
    @Property(
            label = "User id attribute",
            description = "Name of the attribute that contains the user id.",
            value = PARAM_USER_ID_ATTRIBUTE_DEFAULT
    )
    public static final String PARAM_USER_ID_ATTRIBUTE = "user.idAttribute";

    /**
     * @see Identity#getExtraFilter()
     */
    public static final String PARAM_USER_EXTRA_FILTER_DEFAULT = "";

    /**
     * @see Identity#getExtraFilter()
     */
    @Property(
            label = "User extra filter",
            description = "Extra LDAP filter to use when searching for users. The final filter is" +
                    "formatted like: '(&(<idAttr>=<userId>)(objectclass=<objectclass>)<extraFilter>)'",
            value = PARAM_USER_EXTRA_FILTER_DEFAULT
    )
    public static final String PARAM_USER_EXTRA_FILTER = "user.extraFilter";

    /**
     * @see Identity#makeDnPath()
     */
    public static final boolean PARAM_USER_MAKE_DN_PATH_DEFAULT = false;

    /**
     * @see Identity#makeDnPath()
     */
    @Property(
            label = "User DN paths",
            description = "Controls if the DN should be used for calculating a portion of the intermediate path.",
            boolValue = PARAM_USER_MAKE_DN_PATH_DEFAULT
    )
    public static final String PARAM_USER_MAKE_DN_PATH = "user.makeDnPath";

    /**
     * @see Identity#getBaseDN()
     */
    public static final String PARAM_GROUP_BASE_DN_DEFAULT = "ou=groups,o=example,dc=com";

    /**
     * @see Identity#getBaseDN()
     */
    @Property(
            label = "Group base DN",
            description = "The base DN for group searches.",
            value = PARAM_GROUP_BASE_DN_DEFAULT
    )
    public static final String PARAM_GROUP_BASE_DN = "group.baseDN";

    /**
     * @see Identity#getObjectClasses()
     */
    public static final String[] PARAM_GROUP_OBJECTCLASS_DEFAULT = {"groupOfUniqueNames"};

    /**
     * @see Identity#getObjectClasses()
     */
    @Property(
            label = "Group object classes",
            description = "The list of object classes a group entry must contain.",
            value = {"groupOfUniqueNames"},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_GROUP_OBJECTCLASS = "group.objectclass";

    /**
     * @see Identity#getIdAttribute()
     */
    public static final String PARAM_GROUP_NAME_ATTRIBUTE_DEFAULT = "cn";

    /**
     * @see Identity#getIdAttribute()
     */
    @Property(
            label = "Group name attribute",
            description = "Name of the attribute that contains the group name.",
            value = PARAM_GROUP_NAME_ATTRIBUTE_DEFAULT
    )
    public static final String PARAM_GROUP_NAME_ATTRIBUTE = "group.nameAttribute";

    /**
     * @see Identity#getExtraFilter()
     */
    public static final String PARAM_GROUP_EXTRA_FILTER_DEFAULT = "";

    /**
     * @see Identity#getExtraFilter()
     */
    @Property(
            label = "Group extra filter",
            description = "Extra LDAP filter to use when searching for groups. The final filter is" +
                    "formatted like: '(&(<nameAttr>=<groupName>)(objectclass=<objectclass>)<extraFilter>)'",
            value = PARAM_GROUP_EXTRA_FILTER_DEFAULT
    )
    public static final String PARAM_GROUP_EXTRA_FILTER = "group.extraFilter";

    /**
     * @see Identity#makeDnPath()
     */
    public static final boolean PARAM_GROUP_MAKE_DN_PATH_DEFAULT = false;

    /**
     * @see Identity#makeDnPath()
     */
    @Property(
            label = "Group DN paths",
            description = "Controls if the DN should be used for calculating a portion of the intermediate path.",
            boolValue = PARAM_GROUP_MAKE_DN_PATH_DEFAULT
    )
    public static final String PARAM_GROUP_MAKE_DN_PATH = "group.makeDnPath";

    /**
     * @see #getGroupMemberAttribute()
     */
    public static final String PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT = "uniquemember";

    /**
     * @see #getGroupMemberAttribute()
     */
    @Property(
            label = "Group member attribute",
            description = "Group attribute that contains the member(s) of a group.",
            value = PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT
    )
    public static final String PARAM_GROUP_MEMBER_ATTRIBUTE = "group.memberAttribute";

    /**
     * Defines the configuration of an identity (user or group).
     */
    public static class Identity {

        private String baseDN;

        private String[] objectClasses;

        private String idAttribute;

        private String extraFilter;

        private String filterTemplate;

        private boolean makeDnPath;

        /**
         * Configures the base DN for searches of this kind of identity
         * @return the base DN
         */
        @Nonnull
        public String getBaseDN() {
            return baseDN;
        }

        /**
         * Sets the base DN for search of this kind of identity.
         * @param baseDN the DN as string.
         * @return {@code this}
         * @see #getBaseDN()
         */
        @Nonnull
        public Identity setBaseDN(@Nonnull String baseDN) {
            this.baseDN = baseDN;
            return this;
        }

        /**
         * Configures the object classes of this kind of identity.
         * @return an array of object classes
         * @see #getSearchFilter(String) for more detail about searching and filtering
         */
        @Nonnull
        public String[] getObjectClasses() {
            return objectClasses;
        }

        /**
         * Sets the object classes.
         * @param objectClasses the object classes
         * @return {@code this}
         * @see #getObjectClasses()
         */
        @Nonnull
        public Identity setObjectClasses(@Nonnull String ... objectClasses) {
            this.objectClasses = objectClasses;
            filterTemplate = null;
            return this;
        }

        /**
         * Configures the attribute that is used to identify this identity by id. For users this is the attribute that
         * holds the user id, for groups this is the attribute that holds the group name.
         *
         * @return the id attribute name
         * @see #getSearchFilter(String) for more detail about searching and filtering
         */
        @Nonnull
        public String getIdAttribute() {
            return idAttribute;
        }

        /**
         * Sets the id attribute.
         * @param idAttribute the id attribute name
         * @return {@code this}
         * @see #getIdAttribute()
         */
        @Nonnull
        public Identity setIdAttribute(@Nonnull String idAttribute) {
            this.idAttribute = idAttribute;
            filterTemplate = null;
            return this;
        }

        /**
         * Configures the extra LDAP filter that is appended to the internally computed filter when searching for
         * identities.
         *
         * @return the extra filter
         */
        @CheckForNull
        public String getExtraFilter() {
            return extraFilter;
        }

        /**
         * Sets the extra search filter.
         * @param extraFilter the filter
         * @return {@code this}
         * @see #getExtraFilter()
         */
        @Nonnull
        public Identity setExtraFilter(@Nullable String extraFilter) {
            this.extraFilter = extraFilter;
            filterTemplate = null;
            return this;
        }


        /**
         * Configures if the identities DN should be used to generate a portion of the authorizables intermediate path.
         * @return {@code true} if the DN is used a intermediate path.
         */
        public boolean makeDnPath() {
            return makeDnPath;
        }

        /**
         * Sets the intermediate path flag.
         * @param makeDnPath {@code true} to use the DN as intermediate path
         * @return {@code this}
         * @see #makeDnPath()
         */
        @Nonnull
        public Identity setMakeDnPath(boolean makeDnPath) {
            this.makeDnPath = makeDnPath;
            return this;
        }

        /**
         * Returns the LDAP filter that is used when searching this type of identity. The filter is based on the
         * configuration and has the following format:
         *
         * <pre>
         *     (&(${idAttr}=${id})(objectclass=${objectclass})${extraFilter})
         * </pre>
         *
         * Note that the objectclass part is repeated according to the specified objectclasses in {@link #getObjectClasses()}.
         *
         * @param id the id value
         * @return the search filter
         */
        @Nonnull
        public String getSearchFilter(@Nonnull String id) {
            if (filterTemplate == null) {
                StringBuilder filter = new StringBuilder("(&(")
                        .append(idAttribute)
                        .append("=%s)");
                for (String objectClass: objectClasses) {
                    filter.append("(objectclass=")
                            .append(encodeFilterValue(objectClass))
                            .append(')');
                }
                if (extraFilter != null && extraFilter.length() > 0) {
                    filter.append(extraFilter);
                }
                filter.append(')');
                filterTemplate = filter.toString();
            }
            return String.format(filterTemplate, encodeFilterValue(id));
        }

        /**
         * Copied from org.apache.directory.api.ldap.model.filter.FilterEncoder#encodeFilterValue(java.lang.String)
         * in order to keep this configuration LDAP client independent.
         *
         * Handles encoding of special characters in LDAP search filter assertion values using the
         * &lt;valueencoding&gt; rule as described in <a href="http://www.ietf.org/rfc/rfc4515.txt">RFC 4515</a>.
         *
         * @param value Right hand side of "attrId=value" assertion occurring in an LDAP search filter.
         * @return Escaped version of <code>value</code>
         */
        private static String encodeFilterValue(String value) {
            StringBuilder sb = null;
            for (int i = 0; i < value.length(); i++) {
                char ch = value.charAt(i);
                String replace = null;
                switch (ch) {
                    case '*':
                        replace = "\\2A";
                        break;

                    case '(':
                        replace = "\\28";
                        break;

                    case ')':
                        replace = "\\29";
                        break;

                    case '\\':
                        replace = "\\5C";
                        break;

                    case '\0':
                        replace = "\\00";
                        break;
                }
                if (replace != null) {
                    if (sb == null) {
                        sb = new StringBuilder(value.length() * 2);
                        sb.append(value.substring(0, i));
                    }
                    sb.append(replace);
                } else if (sb != null) {
                    sb.append(ch);
                }
            }
            return (sb == null ? value : sb.toString());
        }
    }

    /**
     * Creates a new LDAP provider configuration based on the properties store in the given parameters.
     * @param params the configuration parameters.
     * @return the config
     */
    public static LdapProviderConfig of(ConfigurationParameters params) {
        LdapProviderConfig cfg = new LdapProviderConfig()
                .setName(params.getConfigValue(PARAM_NAME, PARAM_NAME_DEFAULT))
                .setHostname(params.getConfigValue(PARAM_LDAP_HOST, PARAM_LDAP_HOST_DEFAULT))
                .setPort(params.getConfigValue(PARAM_LDAP_PORT, PARAM_LDAP_PORT_DEFAULT))
                .setUseSSL(params.getConfigValue(PARAM_USE_SSL, PARAM_USE_SSL_DEFAULT))
                .setBindDN(params.getConfigValue(PARAM_BIND_DN, PARAM_BIND_DN_DEFAULT))
                .setBindPassword(params.getConfigValue(PARAM_BIND_PASSWORD, PARAM_BIND_PASSWORD_DEFAULT))
                .setSearchTimeout(params.getConfigValue(PARAM_SEARCH_TIMEOUT, PARAM_SEARCH_TIMEOUT_DEFAULT))
                .setGroupMemberAttribute(params.getConfigValue(PARAM_GROUP_MEMBER_ATTRIBUTE, PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT));

        cfg.getUserConfig()
                .setBaseDN(params.getConfigValue(PARAM_USER_BASE_DN, PARAM_USER_BASE_DN))
                .setIdAttribute(params.getConfigValue(PARAM_USER_ID_ATTRIBUTE, PARAM_USER_ID_ATTRIBUTE_DEFAULT))
                .setExtraFilter(params.getConfigValue(PARAM_USER_EXTRA_FILTER, PARAM_USER_EXTRA_FILTER_DEFAULT))
                .setObjectClasses(params.getConfigValue(PARAM_USER_OBJECTCLASS, PARAM_USER_OBJECTCLASS_DEFAULT))
                .setMakeDnPath(params.getConfigValue(PARAM_USER_MAKE_DN_PATH, PARAM_USER_MAKE_DN_PATH_DEFAULT));

        cfg.getGroupConfig()
                .setBaseDN(params.getConfigValue(PARAM_GROUP_BASE_DN, PARAM_GROUP_BASE_DN))
                .setIdAttribute(params.getConfigValue(PARAM_GROUP_NAME_ATTRIBUTE, PARAM_GROUP_NAME_ATTRIBUTE_DEFAULT))
                .setExtraFilter(params.getConfigValue(PARAM_GROUP_EXTRA_FILTER, PARAM_GROUP_EXTRA_FILTER_DEFAULT))
                .setObjectClasses(params.getConfigValue(PARAM_GROUP_OBJECTCLASS, PARAM_GROUP_OBJECTCLASS_DEFAULT))
                .setMakeDnPath(params.getConfigValue(PARAM_GROUP_MAKE_DN_PATH, PARAM_GROUP_MAKE_DN_PATH_DEFAULT));

        return cfg;
    }

    private String name = PARAM_NAME_DEFAULT;

    private String hostname = PARAM_LDAP_HOST_DEFAULT;

    private int port = PARAM_LDAP_PORT_DEFAULT;

    private boolean useSSL = PARAM_USE_SSL_DEFAULT;

    private String bindDN = PARAM_BIND_DN_DEFAULT;

    private String bindPassword = PARAM_BIND_PASSWORD_DEFAULT;

    private int searchTimeout = PARAM_SEARCH_TIMEOUT_DEFAULT;

    private String groupMemberAttribute = PARAM_GROUP_MEMBER_ATTRIBUTE;

    private final Identity userConfig = new Identity()
            .setBaseDN(PARAM_USER_BASE_DN_DEFAULT)
            .setExtraFilter(PARAM_USER_EXTRA_FILTER_DEFAULT)
            .setIdAttribute(PARAM_USER_ID_ATTRIBUTE_DEFAULT)
            .setMakeDnPath(PARAM_USER_MAKE_DN_PATH_DEFAULT)
            .setObjectClasses(PARAM_USER_OBJECTCLASS_DEFAULT);

    private final Identity groupConfig = new Identity()
            .setBaseDN(PARAM_GROUP_BASE_DN_DEFAULT)
            .setExtraFilter(PARAM_GROUP_EXTRA_FILTER_DEFAULT)
            .setIdAttribute(PARAM_GROUP_NAME_ATTRIBUTE_DEFAULT)
            .setMakeDnPath(PARAM_GROUP_MAKE_DN_PATH_DEFAULT)
            .setObjectClasses(PARAM_GROUP_OBJECTCLASS_DEFAULT);

    /**
     * Returns the name of this provider configuration.
     * The default is {@value #PARAM_NAME_DEFAULT}
     *
     * @return the name.
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this provider.
     * @param name the name
     * @return {@code this}
     * @see #getName()
     */
    @Nonnull
    public LdapProviderConfig setName(@Nonnull String name) {
        this.name = name;
        return this;
    }

    /**
     * Configures the hostname of the LDAP server.
     * The default is {@value #PARAM_LDAP_HOST_DEFAULT}
     *
     * @return the hostname
     */
    @Nonnull
    public String getHostname() {
        return hostname;
    }

    /**
     * Sets the hostname.
     * @param hostname the hostname
     * @return {@code this}
     * @see #getHostname()
     */
    @Nonnull
    public LdapProviderConfig setHostname(@Nonnull String hostname) {
        this.hostname = hostname;
        return this;
    }

    /**
     * Configures the port of the LDAP server.
     * The default is {@value #PARAM_LDAP_PORT_DEFAULT}
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port.
     * @param port the port
     * @return {@code this}
     * @see #getPort()
     */
    @Nonnull
    public LdapProviderConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Configures whether SSL connections should be used.
     * The default is {@value #PARAM_USE_SSL_DEFAULT}.
     *
     * @return {@code true} if SSL should be used.
     */
    public boolean useSSL() {
        return useSSL;
    }

    /**
     * Enables SSL connections.
     * @param useSSL {@code true} to enable SSL
     * @return {@code this}
     * @see #useSSL()
     */
    @Nonnull
    public LdapProviderConfig setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
        return this;
    }

    /**
     * Configures the DN that is used to bind to the LDAP server. If this value is {@code null} or an empty string,
     * anonymous connections are used.
     * @return the bind DN or {@code null}.
     */
    @CheckForNull
    public String getBindDN() {
        return bindDN;
    }

    /**
     * Sets the bind DN.
     * @param bindDN the DN
     * @return {@code this}
     * @see #getBindDN()
     */
    @Nonnull
    public LdapProviderConfig setBindDN(@Nullable String bindDN) {
        this.bindDN = bindDN;
        return this;
    }

    /**
     * Configures the password that is used to bind to the LDAP server. This value is not used for anonymous binds.
     * @return the password.
     */
    @CheckForNull
    public String getBindPassword() {
        return bindPassword;
    }

    /**
     * Sets the bind password
     * @param bindPassword the password
     * @return {@code this}
     * @see #getBindPassword()
     */
    @Nonnull
    public LdapProviderConfig setBindPassword(@Nullable String bindPassword) {
        this.bindPassword = bindPassword;
        return this;
    }

    /**
     * Configures the timeout in milliseconds that is used for all LDAP searches.
     * The default is {@value #PARAM_SEARCH_TIMEOUT_DEFAULT}.
     *
     * @return the timeout in milliseconds.
     */
    public int getSearchTimeout() {
        return searchTimeout;
    }

    /**
     * Sets the search timeout.
     * @param searchTimeout the timeout in milliseconds
     * @return {@code this}
     * @see #setSearchTimeout(int)
     */
    @Nonnull
    public LdapProviderConfig setSearchTimeout(int searchTimeout) {
        this.searchTimeout = searchTimeout;
        return this;
    }

    /**
     * Configures the attribute that stores the members of a group.
     * Default is {@value #PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT}
     *
     * @return the group member attribute
     */
    @Nonnull
    public String getGroupMemberAttribute() {
        return groupMemberAttribute;
    }

    /**
     * Sets the group member attribute.
     * @param groupMemberAttribute the attribute name
     * @return {@code this}
     * @see #getGroupMemberAttribute()
     */
    @Nonnull
    public LdapProviderConfig setGroupMemberAttribute(@Nonnull String groupMemberAttribute) {
        this.groupMemberAttribute = groupMemberAttribute;
        return this;
    }

    /**
     * Returns the user specific configuration.
     * @return the user config.
     */
    @Nonnull
    public Identity getUserConfig() {
        return userConfig;
    }

    /**
     * Returns the group specific configuration.
     * @return the groups config.
     */
    @Nonnull
    public Identity getGroupConfig() {
        return groupConfig;
    }
}
