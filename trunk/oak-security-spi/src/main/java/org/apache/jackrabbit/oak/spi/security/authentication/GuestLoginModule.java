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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.io.IOException;
import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code GuestLoginModule} is intended to provide backwards compatibility
 * with the login handling present in the JCR reference implementation located
 * in jackrabbit-core. While the specification claims that {@link javax.jcr.Repository#login}
 * with {@code null} Credentials implies that the authentication process is
 * handled externally, the default implementation jackrabbit-core treated it
 * as 'anonymous' login such as covered by using {@link GuestCredentials}.<p>
 *
 * This {@code LoginModule} implementation performs the following tasks upon
 * {@link #login()}.
 *
 * <ol>
 *     <li>Try to retrieve JCR credentials from the {@link CallbackHandler} using
 *     the {@link CredentialsCallback}</li>
 *     <li>In case no credentials could be obtained it pushes a new instance of
 *     {@link GuestCredentials} to the shared stated. Subsequent login modules
 *     in the authentication process may retrieve the {@link GuestCredentials}
 *     instead of failing to obtain any credentials.</li>
 * </ol>
 *
 * If this login module pushed {@link GuestLoginModule} to the shared state
 * in phase 1 it will add those credentials and the {@link EveryonePrincipal}
 * to the subject in phase 2 of the login process. Subsequent login modules
 * my choose to provide additional principals/credentials associated with
 * a guest login.<p>
 *
 * The authentication configuration using this {@code LoginModule} could for
 * example look as follows:
 *
 * <pre>
 *
 *    jackrabbit.oak {
 *            org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule  optional;
 *            org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
 *    };
 *
 * </pre>
 *
 * In this case calling {@link javax.jcr.Repository#login()} would be equivalent
 * to {@link javax.jcr.Repository#login(javax.jcr.Credentials) repository.login(new GuestCredentials()}.
 */
public final class GuestLoginModule implements LoginModule {

    private static final Logger log = LoggerFactory.getLogger(GuestLoginModule.class);

    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map sharedState;

    private GuestCredentials guestCredentials;

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
    }

    @Override
    public boolean login() {
        if (callbackHandler != null) {
            CredentialsCallback ccb = new CredentialsCallback();
            try {
                callbackHandler.handle(new Callback[] {ccb});
                Credentials credentials = ccb.getCredentials();
                if (credentials == null) {
                    guestCredentials = new GuestCredentials();
                    sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, guestCredentials);
                    return true;
                }
            } catch (IOException e) {
                log.debug("Login: Failed to retrieve Credentials from CallbackHandler", e);
            } catch (UnsupportedCallbackException e) {
                log.debug("Login: Failed to retrieve Credentials from CallbackHandler", e);
            }
        }

        // ignore this login module
        return false;
    }

    @Override
    public boolean commit() {
        if (authenticationSucceeded()) {
            if (!subject.isReadOnly()) {
                subject.getPublicCredentials().add(guestCredentials);
                subject.getPrincipals().add(EveryonePrincipal.getInstance());
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean abort() {
        guestCredentials = null;
        return true;
    }

    @Override
    public boolean logout() {
        return authenticationSucceeded();
    }

    private boolean authenticationSucceeded() {
        return guestCredentials != null;
    }
}
