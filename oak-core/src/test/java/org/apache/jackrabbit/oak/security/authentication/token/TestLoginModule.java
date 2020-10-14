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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TestLoginModule extends AbstractLoginModule {

    private CredentialsSupport credentialsSupport;
    private Credentials credentials;
    private String userId;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        super.initialize(subject, callbackHandler, sharedState, options);

        credentialsSupport = (CredentialsSupport) options.get("credsSupport");
    }

    @NotNull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return new TestCredentialsSupport().getCredentialClasses();
    }

    @Override
    public boolean logout() throws LoginException {
        if (credentials != null) {
            Set<? extends Principal> s = Collections.singleton(EveryonePrincipal.getInstance());
            return logout(ImmutableSet.of(credentials), s);
        } else {
            return false;
        }
    }

    @Override
    public boolean login() {
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
    public boolean commit() {
        if (userId != null) {
            subject.getPrincipals().add(EveryonePrincipal.getInstance());
            AuthInfo info = new AuthInfoImpl(userId, credentialsSupport.getAttributes(credentials), subject.getPrincipals());
            setAuthInfo(info, subject);
            return true;
        } else {
            clearState();
            return false;
        }
    }
}
