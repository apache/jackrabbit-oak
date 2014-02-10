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
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LdapLoginModule extends ExternalLoginModule {

    private static final Logger log = LoggerFactory.getLogger(LdapLoginModule.class);

    private Credentials credentials;
    private LdapUser ldapUser;
    private boolean success;

//    private LdapSearch search;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);
        //TODO
//        search = new JndiLdapSearch(new LdapProviderConfig(options));
    }

    @Override
    public boolean commit() throws LoginException {
        if (success && super.commit()) {
            if (!subject.isReadOnly()) {
                String userId = ldapUser.getId();
                Set<? extends Principal> principals = getPrincipals(userId);

                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(credentials);
                subject.getPublicCredentials().add(createAuthInfo(userId, principals));
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean login() throws LoginException {
        ldapUser = getExternalUser();
//        if (ldapUser != null && search.findUser(ldapUser)) {
//            if (search.authenticate(ldapUser)) {
//                success = true;
//                log.debug("Adding Credentials to shared state.");
//                sharedState.put(SHARED_KEY_CREDENTIALS, credentials);
//
//                log.debug("Adding login name to shared state.");
//                sharedState.put(SHARED_KEY_LOGIN_NAME, ldapUser.getId());
//            }
//        }
        return success;
    }

    //------------------------------------------------< AbstractLoginModule >---
    @Override
    protected void clearState() {
        super.clearState();
        success = false;
        credentials = null;
        ldapUser = null;
//        search = null;
    }

    //------------------------------------------------< ExternalLoginModule >---
    protected boolean loginSucceeded() {
        return success;
    }

    protected LdapUser getExternalUser() {
        if (ldapUser == null) {
            credentials = getCredentials();
            if (credentials instanceof SimpleCredentials) {
                String uid = ((SimpleCredentials) credentials).getUserID();
                char[] pwd = ((SimpleCredentials) credentials).getPassword();
//                return new LdapUser(uid, new String(pwd), search);
            }
        }
        return ldapUser;
    }

    //------------------------------------------------------------< private >---
    private AuthInfo createAuthInfo(@Nonnull String userId, Set<? extends Principal> principals) {
        Map<String, Object> attributes = new HashMap<String, Object>();
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) credentials;
            for (String attrName : sc.getAttributeNames()) {
                attributes.put(attrName, sc.getAttribute(attrName));
            }
        }
        return new AuthInfoImpl(userId, attributes, principals);
    }
}
