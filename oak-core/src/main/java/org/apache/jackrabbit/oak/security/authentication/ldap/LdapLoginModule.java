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

import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;

public final class LdapLoginModule extends ExternalLoginModule {

    private Credentials credentials;
    private LdapUser ldapUser;
    private boolean success;

    private LdapSearch search;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);
        //TODO
        search = new JndiLdapSearch(new LdapSettings(options));
    }

    @Override
    public boolean login() throws LoginException {
        getExternalUser();
        if (ldapUser != null && search.findUser(ldapUser)) {
            search.authenticate(ldapUser);
            success = true;
        }
        return success;
    }

    //------------------------------------------------< AbstractLoginModule >---
    @Override
    protected Credentials getCredentials() {
        if (credentials == null) {
            credentials = super.getCredentials();
        }
        return credentials;
    }

    @Override
    protected void clearState() {
        super.clearState();
        success = false;
        credentials = null;
        ldapUser = null;
        search = null;
    }

    //------------------------------------------------< ExternalLoginModule >---
    @Override
    protected boolean loginSucceeded() {
        return success;
    }

    @Override
    protected ExternalUser getExternalUser() {
        if (ldapUser == null) {
            Credentials creds = getCredentials();
            if (creds instanceof SimpleCredentials) {
                String uid = ((SimpleCredentials) creds).getUserID();
                char[] pwd = ((SimpleCredentials) creds).getPassword();
                ldapUser = new LdapUser(uid, new String(pwd), search);
            }
        }
        return ldapUser;
    }
}
