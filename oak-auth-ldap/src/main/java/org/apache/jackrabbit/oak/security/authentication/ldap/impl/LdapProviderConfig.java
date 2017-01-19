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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.directory.api.util.Strings;
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
            description = "Indicates if an SSL (LDAPs) connection should be used.",
            boolValue = PARAM_USE_SSL_DEFAULT
    )
    public static final String PARAM_USE_SSL = "host.ssl";

    /**
     * @see #useTLS()
     */
    public static final boolean PARAM_USE_TLS_DEFAULT = false;

    /**
     * @see #useTLS()
     */
    @Property(
            label = "Use TLS",
            description = "Indicates if TLS should be started on connections.",
            boolValue = PARAM_USE_TLS_DEFAULT
    )
    public static final String PARAM_USE_TLS = "host.tls";

    /**
     * @see #noCertCheck()
     */
    public static final boolean PARAM_NO_CERT_CHECK_DEFAULT = false;

    /**
     * @see #noCertCheck()
     */
    @Property(
            label = "Disable certificate checking",
            description = "Indicates if server certificate validation should be disabled.",
            boolValue = PARAM_NO_CERT_CHECK_DEFAULT
    )
    public static final String PARAM_NO_CERT_CHECK = "host.noCertCheck";

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
    public static final String PARAM_SEARCH_TIMEOUT_DEFAULT = "60s";

    /**
     * @see #getSearchTimeout()
     */
    @Property(
            label = "Search Timeout",
            description = "Time in until a search times out (eg: '1s' or '1m 30s').",
            value = PARAM_SEARCH_TIMEOUT_DEFAULT
    )
    public static final String PARAM_SEARCH_TIMEOUT = "searchTimeout";

    /**
     * @see PoolConfig#getMaxActive()
     */
    public static final int PARAM_ADMIN_POOL_MAX_ACTIVE_DEFAULT = 8;

    /**
     * @see PoolConfig#getMaxActive()
     */
    @Property(
            label = "Admin pool max active",
            description = "The max active size of the admin connection pool.",
            longValue = PARAM_ADMIN_POOL_MAX_ACTIVE_DEFAULT
    )
    public static final String PARAM_ADMIN_POOL_MAX_ACTIVE = "adminPool.maxActive";

    /**
     * @see PoolConfig#lookupOnValidate()
     */
    public static final boolean PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE_DEFAULT = true;

    /**
     * @see PoolConfig#lookupOnValidate()
     */
    @Property(
            label = "Admin pool lookup on validate",
            description = "Indicates an ROOT DSE lookup is performed to test if the connection is still valid when taking it out of the pool.",
            boolValue = PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE_DEFAULT
    )
    public static final String PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE = "adminPool.lookupOnValidate";

    /**
     * @see PoolConfig#getMaxActive()
     */
    public static final int PARAM_USER_POOL_MAX_ACTIVE_DEFAULT = 8;

    /**
     * @see PoolConfig#getMaxActive()
     */
    @Property(
            label = "User pool max active",
            description = "The max active size of the user connection pool.",
            longValue = PARAM_USER_POOL_MAX_ACTIVE_DEFAULT
    )
    public static final String PARAM_USER_POOL_MAX_ACTIVE = "userPool.maxActive";

    /**
     * @see PoolConfig#lookupOnValidate()
     */
    public static final boolean PARAM_USER_POOL_LOOKUP_ON_VALIDATE_DEFAULT = true;

    /**
     * @see PoolConfig#lookupOnValidate()
     */
    @Property(
            label = "User pool lookup on validate",
            description = "Indicates an ROOT DSE lookup is performed to test if the connection is still valid when taking it out of the pool.",
            boolValue = PARAM_USER_POOL_LOOKUP_ON_VALIDATE_DEFAULT
    )
    public static final String PARAM_USER_POOL_LOOKUP_ON_VALIDATE = "userPool.lookupOnValidate";

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
     * @see Identity#getCustomAttributes()
     */
    public static final String[] PARAM_CUSTOM_ATTRIBUTES_DEFAULT = {};

    /**
     * @see Identity#getCustomAttributes()
     */
    @Property(
            label = "Custom Attributes",
            description = "Attributes retrieved when looking up LDAP entries. Leave empty to retrieve all attributes.",
            value = {},
            cardinality = Integer.MAX_VALUE
    )
    public static final String PARAM_CUSTOM_ATTRIBUTES = "customattributes";

    /**
     * Defines the configuration of an identity (user or group).
     */
    public class Identity {

        private String baseDN;

        private String[] objectClasses;

        private String idAttribute;

        private String[] customAttributes = {};

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
            memberOfFilterTemplate = null;
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
            memberOfFilterTemplate = null;
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
            memberOfFilterTemplate = null;
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
         * <pre>{@code
         *     (&(${idAttr}=${id})(objectclass=${objectclass})${extraFilter})
         * }</pre>
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Identity{");
            sb.append("baseDN='").append(baseDN).append('\'');
            sb.append(", objectClasses=").append(Arrays.toString(objectClasses));
            sb.append(", idAttribute='").append(idAttribute).append('\'');
            sb.append(", userAttributes='").append(Arrays.toString(customAttributes));
            sb.append(", extraFilter='").append(extraFilter).append('\'');
            sb.append(", filterTemplate='").append(filterTemplate).append('\'');
            sb.append(", makeDnPath=").append(makeDnPath);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Defines the configuration of a connection pool. Currently we only define the max size.
     * (documentation copied from {@link org.apache.commons.pool.impl.GenericObjectPool})
     */
    public static class PoolConfig {

        private int maxActiveSize;

        private boolean lookupOnValidate;

        /**
         * Returns the maximum number of objects that can be allocated by the pool
         * (checked out to clients, or idle awaiting checkout) at a given time.
         * When non-positive, there is no limit to the number of objects that can
         * be managed by the pool at one time. A value of 0 disables this pool.
         *
         * @return the cap on the total number of object instances managed by the pool.
         * @see #setMaxActive
         */
        public int getMaxActive() {
            return maxActiveSize;
        }

        /**
         * Sets the cap on the number of objects that can be allocated by the pool.
         *
         * @see #getMaxActive
         * @return this
         */
        @Nonnull
        public PoolConfig setMaxActive(int maxActive) {
            this.maxActiveSize = maxActive;
            return this;
        }

        /**
         * Defines if the lookup on validate flag is enabled. If enable a connection that taken from the
         * pool are validated before used. currently this is done by performing a lookup to the ROOT DSE, which
         * might not be allowed on all LDAP servers.

         * @return {@code true} if the flag is enabled.
         */
        public boolean lookupOnValidate() {
            return lookupOnValidate;
        }

        /**
         * Sets the lookup on validate flag.
         *
         * @see #lookupOnValidate()
         * @return this
         */
        @Nonnull
        public PoolConfig setLookupOnValidate(boolean lookupOnValidate) {
            this.lookupOnValidate = lookupOnValidate;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PoolConfig{");
            sb.append("maxActiveSize=").append(maxActiveSize);
            sb.append(", lookupOnValidate=").append(lookupOnValidate);
            sb.append('}');
            return sb.toString();
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
                .setUseTLS(params.getConfigValue(PARAM_USE_TLS, PARAM_USE_TLS_DEFAULT))
                .setNoCertCheck(params.getConfigValue(PARAM_NO_CERT_CHECK, PARAM_NO_CERT_CHECK_DEFAULT))
                .setBindDN(params.getConfigValue(PARAM_BIND_DN, PARAM_BIND_DN_DEFAULT))
                .setBindPassword(params.getConfigValue(PARAM_BIND_PASSWORD, PARAM_BIND_PASSWORD_DEFAULT))
                .setGroupMemberAttribute(params.getConfigValue(PARAM_GROUP_MEMBER_ATTRIBUTE, PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT))
                .setCustomAttributes(params.getConfigValue(PARAM_CUSTOM_ATTRIBUTES, PARAM_CUSTOM_ATTRIBUTES_DEFAULT));

        ConfigurationParameters.Milliseconds ms = ConfigurationParameters.Milliseconds.of(params.getConfigValue(PARAM_SEARCH_TIMEOUT, PARAM_SEARCH_TIMEOUT_DEFAULT));
        if (ms != null) {
            cfg.setSearchTimeout(ms.value);
        }

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

        cfg.getAdminPoolConfig()
                .setLookupOnValidate(params.getConfigValue(PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE, PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE_DEFAULT))
                .setMaxActive(params.getConfigValue(PARAM_ADMIN_POOL_MAX_ACTIVE, PARAM_ADMIN_POOL_MAX_ACTIVE_DEFAULT));

        cfg.getUserPoolConfig()
                .setLookupOnValidate(params.getConfigValue(PARAM_USER_POOL_LOOKUP_ON_VALIDATE, PARAM_USER_POOL_LOOKUP_ON_VALIDATE_DEFAULT))
                .setMaxActive(params.getConfigValue(PARAM_USER_POOL_MAX_ACTIVE, PARAM_USER_POOL_MAX_ACTIVE_DEFAULT));

        return cfg;
    }

    private String name = PARAM_NAME_DEFAULT;

    private String hostname = PARAM_LDAP_HOST_DEFAULT;

    private int port = PARAM_LDAP_PORT_DEFAULT;

    private boolean useSSL = PARAM_USE_SSL_DEFAULT;

    private boolean useTLS = PARAM_USE_TLS_DEFAULT;

    private boolean noCertCheck = PARAM_NO_CERT_CHECK_DEFAULT;

    private String bindDN = PARAM_BIND_DN_DEFAULT;

    private String bindPassword = PARAM_BIND_PASSWORD_DEFAULT;

    private long searchTimeout = ConfigurationParameters.Milliseconds.of(PARAM_SEARCH_TIMEOUT_DEFAULT).value;

    private String groupMemberAttribute = PARAM_GROUP_MEMBER_ATTRIBUTE;

    private String memberOfFilterTemplate;

    private String[] customAttributes = PARAM_CUSTOM_ATTRIBUTES_DEFAULT;

    private final PoolConfig adminPoolConfig = new PoolConfig()
            .setMaxActive(PARAM_ADMIN_POOL_MAX_ACTIVE_DEFAULT);

    private final PoolConfig userPoolConfig = new PoolConfig()
            .setMaxActive(PARAM_USER_POOL_MAX_ACTIVE_DEFAULT);

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
     * Configures whether TLS connections should be used.
     * The default is {@value #PARAM_USE_TLS_DEFAULT}.
     *
     * @return {@code true} if TLS should be used.
     */
    public boolean useTLS() {
        return useTLS;
    }

    /**
     * Enables TLS connections.
     * @param useTLS {@code true} to enable TLS
     * @return {@code this}
     * @see #useTLS()
     */
    @Nonnull
    public LdapProviderConfig setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
        return this;
    }

    /**
     * Configures whether certificates on SSL/TLS connections should be validated.
     * The default is {@value #PARAM_NO_CERT_CHECK_DEFAULT}.
     *
     * @return {@code true} if certificates should not be validated
     */
    public boolean noCertCheck() {
        return noCertCheck;
    }

    /**
     * Disables certificate validation.
     * @param noCertCheck {@code true} to disable certificate validation
     * @return {@code this}
     * @see #noCertCheck()
     */
    @Nonnull
    public LdapProviderConfig setNoCertCheck(boolean noCertCheck) {
        this.noCertCheck = noCertCheck;
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
    public long getSearchTimeout() {
        return searchTimeout;
    }

    /**
     * Sets the search timeout.
     * @param searchTimeout the timeout in milliseconds
     * @return {@code this}
     * @see #getSearchTimeout()
     */
    @Nonnull
    public LdapProviderConfig setSearchTimeout(long searchTimeout) {
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
     * Optionally configures an array of attribute names that will be retrieved when looking up LDAP entries.
     * Defaults to the empty array indicating that all attributes will be retrieved.
     *
     * @return an array of attribute names. The empty array indicates that all attributes will be retrieved.
     */
    @Nonnull
    public String[] getCustomAttributes() {
        return customAttributes;
    }

    /**
     * Sets the attribute names to be retrieved when looking up LDAP entries. The empty array indicates that all attributes will be retrieved.
     *
     * @param customAttributes an array of attribute names
     * @return the Identity instance
     */
    @Nonnull
    public LdapProviderConfig setCustomAttributes(@Nonnull String[] customAttributes) {
        this.customAttributes = this.removeEmptyStrings(customAttributes);
        return this;
    }

    /**
     * Returns the LDAP filter that is used when searching for groups where an identity is member of.
     * The filter is based on the configuration and has the following format:
     *
     * <pre>{@code
     *     (&(${memberAttribute}=${dn})(objectclass=${objectclass})${extraFilter})
     * }</pre>
     *
     * Note that the objectclass part is repeated according to the specified objectclasses in
     * {@link Identity#getObjectClasses()} of the group configuration.
     *
     * @param dn the dn of the identity to search for
     * @return the search filter
     */
    public String getMemberOfSearchFilter(@Nonnull String dn) {
        if (memberOfFilterTemplate == null) {
            StringBuilder filter = new StringBuilder("(&(")
                    .append(groupMemberAttribute)
                    .append("=%s)");
            for (String objectClass: groupConfig.objectClasses) {
                filter.append("(objectclass=")
                        .append(encodeFilterValue(objectClass))
                        .append(')');
            }
            if (groupConfig.extraFilter != null && groupConfig.extraFilter.length() > 0) {
                filter.append(groupConfig.extraFilter);
            }
            filter.append(')');
            memberOfFilterTemplate = filter.toString();
        }
        return String.format(memberOfFilterTemplate, encodeFilterValue(dn));
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

    /**
     * Returns the admin connection pool configuration.
     * @return admin pool config
     */
    @Nonnull
    public PoolConfig getAdminPoolConfig() {
        return adminPoolConfig;
    }

    /**
     * Returns the user connection pool configuration.
     * @return user pool config
     */
    @Nonnull
    public PoolConfig getUserPoolConfig() {
        return userPoolConfig;
    }

    /**
     * Copied from org.apache.directory.api.ldap.model.filter.FilterEncoder#encodeFilterValue(java.lang.String)
     * in order to keep this configuration LDAP client independent.
     *
     * Handles encoding of special characters in LDAP search filter assertion values using the
     * &lt;valueencoding&gt; rule as described in <a href="http://www.ietf.org/rfc/rfc4515.txt">RFC 4515</a>.
     *
     * @param value Right hand side of "attrId=value" assertion occurring in an LDAP search filter.
     * @return Escaped version of {@code value}
     */
    public static String encodeFilterValue(String value) {
        StringBuilder sb = null;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            String replace;
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

                default:
                    replace = null;
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

    //OAK-5490
    private String[] removeEmptyStrings(@Nonnull String[] params) {
        List<String> list = Arrays.asList(params);
        if (!list.contains(Strings.EMPTY_STRING)) {
            return params;
        }
        List<String> resultList = new LinkedList<>(list);
        while (resultList.contains(Strings.EMPTY_STRING)) {
            resultList.remove(Strings.EMPTY_STRING);
        }
        String[] result = new String[resultList.size()];
        return resultList.toArray(result);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LdapProviderConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", hostname='").append(hostname).append('\'');
        sb.append(", port=").append(port);
        sb.append(", useSSL=").append(useSSL);
        sb.append(", useTLS=").append(useTLS);
        sb.append(", noCertCheck=").append(noCertCheck);
        sb.append(", bindDN='").append(bindDN).append('\'');
        sb.append(", bindPassword='***'");
        sb.append(", searchTimeout=").append(searchTimeout);
        sb.append(", groupMemberAttribute='").append(groupMemberAttribute).append('\'');
        sb.append(", memberOfFilterTemplate='").append(memberOfFilterTemplate).append('\'');
        sb.append(", adminPoolConfig=").append(adminPoolConfig);
        sb.append(", userPoolConfig=").append(userPoolConfig);
        sb.append(", userConfig=").append(userConfig);
        sb.append(", groupConfig=").append(groupConfig);
        sb.append('}');
        return sb.toString();
    }
}
