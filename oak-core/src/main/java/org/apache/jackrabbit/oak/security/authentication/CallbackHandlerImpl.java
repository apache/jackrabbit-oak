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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.WhiteboardCallback;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * Default implementation of the {@link CallbackHandler} interface. It currently
 * supports the following {@code Callback} implementations:
 * <p>
 * <ul>
 * <li>{@link CredentialsCallback}</li>
 * <li>{@link NameCallback}</li>
 * <li>{@link PasswordCallback}</li>
 * <li>{@link RepositoryCallback}</li>
 * </ul>
 */
class CallbackHandlerImpl implements CallbackHandler {

    private final Credentials credentials;
    private final String workspaceName;
    private final ContentRepository contentRepository;
    private final SecurityProvider securityProvider;
    private final Whiteboard whiteboard;

    CallbackHandlerImpl(Credentials credentials, String workspaceName,
                        ContentRepository contentRepository,
                        SecurityProvider securityProvider,
                        Whiteboard whiteboard) {
        this.credentials = credentials;
        this.workspaceName = workspaceName;
        this.contentRepository = contentRepository;
        this.securityProvider = securityProvider;
        this.whiteboard = whiteboard;
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
            } else if (callback instanceof RepositoryCallback) {
                RepositoryCallback repositoryCallback = (RepositoryCallback) callback;
                repositoryCallback.setContentRepository(contentRepository);
                repositoryCallback.setSecurityProvider(securityProvider);
                repositoryCallback.setWorkspaceName(workspaceName);
            } else if (callback instanceof WhiteboardCallback) {
                ((WhiteboardCallback) callback).setWhiteboard(whiteboard);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    //------------------------------------------------------------< private >---

    private String getName() {
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