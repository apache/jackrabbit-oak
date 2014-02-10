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
package org.apache.jackrabbit.oak.security.authentication.ldap;

import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.filter.FilterEncoder;
import org.apache.directory.api.ldap.model.name.Dn;

/**
 * Configuration of the ldap provider.
 */
public class LdapProviderConfig {

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

    private String host = "localhost";
    private int port = 389;
    private boolean secure = false;
    private String authDn = "";
    private String authPw = "";
    private int searchTimeout = 60000;

    private final Identity userConfig = new Identity();
    private final Identity groupConfig = new Identity();

    private String groupMembershipAttribute = "uniquemember";

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

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public String getAuthDn() {
        return authDn;
    }

    public void setAuthDn(String authDn) {
        this.authDn = authDn;
    }

    public String getAuthPw() {
        return authPw;
    }

    public void setAuthPw(String authPw) {
        this.authPw = authPw;
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
