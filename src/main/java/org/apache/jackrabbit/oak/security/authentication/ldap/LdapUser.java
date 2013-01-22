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

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;

public class LdapUser implements ExternalUser {

    private final String uid;
    private final String pwd;
    private final LdapSearch search;

    private String path;
    private String dn;
    private Principal principal;
    private Set<LdapGroup> groups;
    private Map<String, ?> properties = new HashMap<String, Object>();

    public LdapUser(String uid, String pwd, LdapSearch search) {
        this.uid = uid;
        this.pwd = pwd;
        this.search = search;
    }

    @Override
    public String getId() {
        return this.uid;
    }

    @Override
    public String getPassword() {
        return this.pwd;
    }

    @Override
    public Principal getPrincipal() {
        if (this.principal == null) {
            this.principal = new PrincipalImpl(this.uid);
        }
        return this.principal;
    }

    @Override
    public String getPath() {
        //TODO also support splitdn mode
        if (this.path == null) {
            this.path = this.getDN();
        }
        return this.path;
    }

    @Override
    public Set<LdapGroup> getGroups() {
        if (this.groups == null) {
            this.groups = this.search.findGroups(this);
        }
        return this.groups;
    }

    @Override
    public Map<String, ?> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String, ?> properties) {
        this.properties = properties;
    }

    public String getDN() {
        return this.dn;
    }

    public void setDN(String dn) {
        this.dn = dn;
    }
}
