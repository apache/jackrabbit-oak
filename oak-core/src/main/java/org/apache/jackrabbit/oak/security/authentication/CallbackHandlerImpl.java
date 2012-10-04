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

import java.io.IOException;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.jackrabbit.oak.spi.security.authentication.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link CallbackHandler} interface. It currently
 * supports the following {@code Callback} implementations:
 *
 * <ul>
 *     <li>{@link CredentialsCallback}</li>
 *     <li>{@link NameCallback}</li>
 *     <li>{@link PasswordCallback}</li>
 *     <li>{@link PrincipalProviderCallback}</li>
 * </ul>
 */
public class CallbackHandlerImpl implements CallbackHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CallbackHandlerImpl.class);

    private final Credentials credentials;
    private final String workspaceName;
    private final NodeStore nodeStore;
    private final PrincipalProvider principalProvider;

    public CallbackHandlerImpl(Credentials credentials, String workspaceName,
                               NodeStore nodeStore, PrincipalProvider principalProvider) {
        this.credentials = credentials;
        this.workspaceName = workspaceName;
        this.nodeStore = nodeStore;
        this.principalProvider = principalProvider;
    }

    //----------------------------------------------------< CallbackHandler >---
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof CredentialsCallback) {
                ((CredentialsCallback) callback).setCredentials(credentials);
            } else if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(getName());
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(getPassword());
            } else if (callback instanceof PrincipalProviderCallback) {
                ((PrincipalProviderCallback) callback).setPrincipalProvider(principalProvider);
            } else if (callback instanceof RepositoryCallback) {
                RepositoryCallback repositoryCallback = (RepositoryCallback) callback;
                repositoryCallback.setNodeStore(nodeStore);
                repositoryCallback.setWorkspaceName(workspaceName);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    //------------------------------------------------------------< private >---

    private String getName(){
        if (credentials instanceof SimpleCredentials) {
            return ((SimpleCredentials) credentials).getUserID();
        } else {
            return null;
        }
    }

    private char[] getPassword() {
        if (credentials instanceof SimpleCredentials) {
            return ((SimpleCredentials) credentials).getPassword();
        } else {
            return null;
        }
    }
}