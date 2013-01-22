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

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class LdapLoginModule extends ExternalLoginModule {

    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModule.class);

    private Credentials credentials;
    private LdapUser ldapUser;
    private boolean success;

    private LdapSearch search;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);
        //TODO
        this.search = new JndiLdapSearch(new LdapSettings(options));
    }

    @Override
    protected boolean loginSucceeded() {
        return this.success;
    }

    @Override
    protected ExternalUser getExternalUser() {
        if (this.ldapUser == null) {
            Credentials creds = getCredentials();
            if (creds instanceof SimpleCredentials) {
                String uid = ((SimpleCredentials) creds).getUserID();
                char[] pwd = ((SimpleCredentials) creds).getPassword();
                this.ldapUser = new LdapUser(uid, new String(pwd), this.search);
            }
        }
        return this.ldapUser;
    }

    @Override
    public boolean login() throws LoginException {
        getExternalUser();
        if (this.ldapUser != null && this.search.findUser(this.ldapUser)) {
            this.search.authenticate(this.ldapUser);
            this.success = true;
        }
        return this.success;
    }

    @Override
    protected Credentials getCredentials() {
        if (this.credentials == null) {
            this.credentials = super.getCredentials();
        }
        return this.credentials;
    }

    @Override
    protected void clearState() {
        super.clearState();
        this.success = false;
        this.credentials = null;
        this.ldapUser = null;
        this.search = null;
    }
}
