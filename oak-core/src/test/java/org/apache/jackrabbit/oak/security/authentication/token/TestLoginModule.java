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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

public class TestLoginModule extends AbstractLoginModule {

    private CredentialsSupport credentialsSupport;
    private Credentials credentials;
    private String userId;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);

        credentialsSupport = (CredentialsSupport) options.get("credsSupport");
    }

    @Nonnull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return new TestCredentialsSupport().getCredentialClasses();
    }

    @Override
    public boolean login() throws LoginException {
        credentials = getCredentials();
        if (credentials != null) {
            userId = credentialsSupport.getUserId(credentials);
            sharedState.put(SHARED_KEY_CREDENTIALS, credentials);
            sharedState.put(SHARED_KEY_LOGIN_NAME, credentialsSupport.getUserId(credentials));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean commit() throws LoginException {
        if (userId != null) {
            subject.getPrincipals().add(EveryonePrincipal.getInstance());
            setAuthInfo(new AuthInfoImpl(userId, credentialsSupport.getAttributes(credentials), subject.getPrincipals()), subject);
            return true;
        } else {
            return false;
        }
    }
}