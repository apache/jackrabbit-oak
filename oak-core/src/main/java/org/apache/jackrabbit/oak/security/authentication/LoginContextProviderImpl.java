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

import java.security.AccessController;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.JaasLoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthContext;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LoginContextProvider}
 */
class LoginContextProviderImpl implements LoginContextProvider {

    private static final Logger log = LoggerFactory.getLogger(LoginContextProviderImpl.class);

    private final String appName;
    private final Configuration configuration;
    private final ContentRepository contentRepository;
    private final SecurityProvider securityProvider;
    private final Whiteboard whiteboard;

    LoginContextProviderImpl(String appName, Configuration configuration,
                             ContentRepository contentRepository,
                             SecurityProvider securityProvider,
                             Whiteboard whiteboard) {
        this.appName = appName;
        this.configuration = configuration;
        this.contentRepository = contentRepository;
        this.securityProvider = securityProvider;
        this.whiteboard = whiteboard;
    }

    @Override
    @Nonnull
    public LoginContext getLoginContext(Credentials credentials, String workspaceName)
            throws LoginException {
        Subject subject = getSubject();
        if (subject != null && credentials == null) {
            log.debug("Found pre-authenticated subject: No further login actions required.");
            return new PreAuthContext(subject);
        }

        if (subject == null) {
            subject = new Subject();
        }
        CallbackHandler handler = getCallbackHandler(credentials, workspaceName);
        return new JaasLoginContext(appName, subject, handler, configuration);
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private static Subject getSubject() {
        Subject subject = null;
        try {
            subject = Subject.getSubject(AccessController.getContext());
        } catch (SecurityException e) {
            log.debug("Can't check for pre-authenticated subject. Reason:", e.getMessage());
        }
        return subject;
    }

    @Nonnull
    private CallbackHandler getCallbackHandler(Credentials credentials, String workspaceName) {
        return new CallbackHandlerImpl(credentials, workspaceName, contentRepository, securityProvider, whiteboard);
    }
}