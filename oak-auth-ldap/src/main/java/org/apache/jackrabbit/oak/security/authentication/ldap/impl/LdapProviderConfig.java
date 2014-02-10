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

import javax.annotation.Nonnull;

import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.filter.FilterEncoder;
import org.apache.directory.api.ldap.model.name.Dn;
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

    public static final String PARAM_NAME_DEFAULT = "ldap";
    @Property(
            label = "LDAP Provider Name",
            description = "Name of this LDAP provider configuration. This is used to reference this provider by the login modules.",
            value = PARAM_NAME_DEFAULT
    )
    public static final String PARAM_NAME = "provider.name";

    public static final String PARAM_LDAP_HOST_DEFAULT = "localhost";
    @Property(
            label = "LDAP Server Hostname",
            description = "Hostname of the LDAP server",
            value = PARAM_LDAP_HOST_DEFAULT
    )
    public static final String PARAM_LDAP_HOST = "host.name";

    public static final int PARAM_LDAP_PORT_DEFAULT = 389;
    @Property(
            label = "LDAP Server Port",
            description = "Port of the LDAP server",
            intValue = PARAM_LDAP_PORT_DEFAULT
    )
    public static final String PARAM_LDAP_PORT = "host.port";

    public static final boolean PARAM_USE_SSL_DEFAULT = false;
    @Property(
            label = "Use SSL",
            description = "Indicates if an SSL connection should be used.",
            boolValue = PARAM_USE_SSL_DEFAULT
    )
    public static final String PARAM_USE_SSL = "host.ssl";

    public static final String PARAM_BIND_DN_DEFAULT = "";
    @Property(
            label = "Bind DN",
            description = "DN of the user for authentication. Leave empty for anonymous bind.",
            value = PARAM_BIND_DN_DEFAULT
    )
    public static final String PARAM_BIND_DN = "bind.dn";

    public static final String PARAM_BIND_PASSWORD_DEFAULT = "";
    @Property(
            label = "Bind Password",
            description = "Password of the user for authentication.",
            passwordValue = PARAM_BIND_PASSWORD_DEFAULT
    )
    public static final String PARAM_BIND_PASSWORD = "bind.password";

    public static final int PARAM_SEARCH_TIMEOUT_DEFAULT = 60000;
    @Property(
            label = "Search Timeout",
            description = "Time in milliseconds until a search times out.",
            intValue = PARAM_SEARCH_TIMEOUT_DEFAULT
    )
    public static final String PARAM_SEARCH_TIMEOUT = "search.timeout";

    /**
     * Defines the configuration of an identity.
     */
    public static class Identity {

        private Dn baseDN;

        private String[] objectClasses;

        private String idAttribute;

        private String extraFilter;

        private String filterTemplate;

        public Dn getBaseDN() {
            return baseDN;
        }

        public void setBaseDN(String baseDN) throws LdapInvalidDnException {
            this.baseDN = new Dn(baseDN);
        }

        public String[] getObjectClasses() {
            return objectClasses;
        }

        public void setObjectClasses(String[] objectClasses) {
            this.objectClasses = objectClasses;
            filterTemplate = null;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public void setIdAttribute(String idAttribute) {
            this.idAttribute = idAttribute;
            filterTemplate = null;
        }

        public String getExtraFilter() {
            return extraFilter;
        }

        public void setExtraFilter(String extraFilter) {
            this.extraFilter = extraFilter;
            filterTemplate = null;
        }

        public String getSearchFilter(String id) {
            if (filterTemplate == null) {
                StringBuilder filter = new StringBuilder("(&(")
                        .append(idAttribute)
                        .append("=%s)");
                for (String objectClass: objectClasses) {
                    filter.append("(objectclass=")
                            .append(FilterEncoder.encodeFilterValue(objectClass))
                            .append(')');
                }
                if (extraFilter != null && extraFilter.length() > 0) {
                    filter.append(extraFilter);
                }
                filter.append(')');
                filterTemplate = filter.toString();
            }
            return String.format(filterTemplate, FilterEncoder.encodeFilterValue(id));
        }
    }

    public static LdapProviderConfig of(ConfigurationParameters params) {
        LdapProviderConfig cfg = new LdapProviderConfig();
        cfg.name = params.getConfigValue(PARAM_NAME, cfg.name);
        cfg.host = params.getConfigValue(PARAM_LDAP_HOST, PARAM_LDAP_HOST_DEFAULT);
        cfg.port = params.getConfigValue(PARAM_LDAP_PORT, PARAM_LDAP_PORT_DEFAULT);
        cfg.useSSL = params.getConfigValue(PARAM_USE_SSL, PARAM_USE_SSL_DEFAULT);
        cfg.bindDN = params.getConfigValue(PARAM_BIND_DN, PARAM_BIND_DN_DEFAULT);
        cfg.bindPassword = params.getConfigValue(PARAM_BIND_PASSWORD, PARAM_BIND_PASSWORD_DEFAULT);
        cfg.searchTimeout = params.getConfigValue(PARAM_SEARCH_TIMEOUT, PARAM_SEARCH_TIMEOUT_DEFAULT);
        return cfg;
    }


    private String name = "ldap";
    private String host = PARAM_LDAP_HOST_DEFAULT;
    private int port = PARAM_LDAP_PORT_DEFAULT;
    private boolean useSSL = PARAM_USE_SSL_DEFAULT;
    private String bindDN = PARAM_BIND_DN_DEFAULT;
    private String bindPassword = PARAM_BIND_PASSWORD_DEFAULT;
    private int searchTimeout = PARAM_SEARCH_TIMEOUT_DEFAULT;

    private final Identity userConfig = new Identity();
    private final Identity groupConfig = new Identity();

    private String groupMembershipAttribute = "uniquemember";

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getBindDN() {
        return bindDN;
    }

    public void setBindDN(String bindDN) {
        this.bindDN = bindDN;
    }

    public String getBindPassword() {
        return bindPassword;
    }

    public void setBindPassword(String bindPassword) {
        this.bindPassword = bindPassword;
    }

    public int getSearchTimeout() {
        return searchTimeout;
    }

    public void setSearchTimeout(int searchTimeout) {
        this.searchTimeout = searchTimeout;
    }

    public String getGroupMembershipAttribute() {
        return groupMembershipAttribute;
    }

    public void setGroupMembershipAttribute(String groupMembershipAttribute) {
        this.groupMembershipAttribute = groupMembershipAttribute;
    }

    public Identity getUserConfig() {
        return userConfig;
    }

    public Identity getGroupConfig() {
        return groupConfig;
    }
}
