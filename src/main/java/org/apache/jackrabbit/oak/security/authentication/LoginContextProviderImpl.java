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
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.JaasLoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LoginContextProvider}
 */
public class LoginContextProviderImpl implements LoginContextProvider {

    private static final Logger log = LoggerFactory.getLogger(LoginContextProviderImpl.class);

    private final String appName;
    private final Configuration configuration;
    private final NodeStore nodeStore;
    private final QueryIndexProvider indexProvider;
    private final SecurityProvider securityProvider;

    public LoginContextProviderImpl(String appName, Configuration configuration,
                                    NodeStore nodeStore, QueryIndexProvider indexProvider,
                                    SecurityProvider securityProvider) {
        this.appName = appName;
        this.configuration = configuration;
        this.nodeStore = nodeStore;
        this.indexProvider = indexProvider;
        this.securityProvider = securityProvider;
    }

    @Override
    @Nonnull
    public LoginContext getLoginContext(Credentials credentials, String workspaceName)
            throws LoginException {
        Subject subject = getSubject();
        CallbackHandler handler = getCallbackHandler(credentials, workspaceName);
        return new JaasLoginContext(appName, subject, handler, configuration);
    }

    //------------------------------------------------------------< private >---
    private static Subject getSubject() {
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

    private CallbackHandler getCallbackHandler(Credentials credentials, String workspaceName) {
        return new CallbackHandlerImpl(credentials, workspaceName, nodeStore, indexProvider, securityProvider);
    }
}