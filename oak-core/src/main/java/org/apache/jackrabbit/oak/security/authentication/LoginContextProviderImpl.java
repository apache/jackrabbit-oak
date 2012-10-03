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
package org.apache.jackrabbit.oak.security.authentication;

import org.apache.jackrabbit.oak.security.principal.TmpPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.JaasLoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.OakLoginContext;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import java.security.AccessController;

/**
 * LoginContextProviderImpl...  TODO
 */
public class LoginContextProviderImpl implements LoginContextProvider {

    private static final Logger log = LoggerFactory.getLogger(LoginContextProviderImpl.class);

    private static final String APP_NAME = "jackrabbit.oak";

    private final Configuration authConfig;
    private final PrincipalProvider principalProvider;

    public LoginContextProviderImpl() {
        // TODO: use configurable authentication config and principal provider
        authConfig = new ConfigurationImpl();
        principalProvider = new TmpPrincipalProvider();
    }

    @Override @Nonnull
    public OakLoginContext getLoginContext(
            Credentials credentials, String workspaceName)
            throws LoginException {
        // TODO: add proper implementation
        // TODO  - authentication against configurable spi-authentication
        // TODO  - validation of workspace name (including access rights for the given 'user')
        Subject subject = getSubject();
        CallbackHandler handler =
                new CallbackHandlerImpl(credentials, principalProvider);
        return new JaasLoginContext(APP_NAME, subject, handler, authConfig);
    }

    //-------------------------------------------------===--------< private >---
    private Subject getSubject() {
        Subject subject = null;
        try {
            subject = Subject.getSubject(AccessController.getContext());
        } catch (SecurityException e) {
            log.debug("Can't check for pre-authentication. Reason:", e.getMessage());
        }
        if (subject == null) {
            subject = new Subject();
        }
        return subject;
    }
}