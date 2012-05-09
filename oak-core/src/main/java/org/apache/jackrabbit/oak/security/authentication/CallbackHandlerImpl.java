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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

/**
 * CallbackHandlerImpl...
 */
public class CallbackHandlerImpl implements CallbackHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CallbackHandlerImpl.class);

    private final Credentials credentials;

    public CallbackHandlerImpl(Credentials credentials) {
        this.credentials = credentials;
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
            } else if (callback instanceof ImpersonationCallback) {
                ((ImpersonationCallback) callback).setImpersonator(getImpersonationSubject());
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    //--------------------------------------------------------------------------

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

    private Subject getImpersonationSubject() {
        if (credentials instanceof ImpersonationCredentials) {
            return ((ImpersonationCredentials) credentials).getImpersonatingSubject();
        } else if (credentials instanceof SimpleCredentials) {
            Object attr = ((SimpleCredentials) credentials).getAttribute(ImpersonationCredentials.IMPERSONATOR_ATTRIBUTE);
            if (attr instanceof Subject) {
                return (Subject) attr;
            }
        }

        return null;
    }
}