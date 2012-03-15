/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.security;


import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.LoginException;
import javax.jcr.SimpleCredentials;
import java.util.Arrays;

import static java.text.MessageFormat.format;

public class AuthenticatorImpl implements Authenticator {
    public static final Authenticator INSTANCE = new AuthenticatorImpl();

    private AuthenticatorImpl() {}

    @Override
    public CredentialsInfo authenticate(Credentials credentials) throws LoginException {
        // todo implement authentication, split into aggregate of SimpleAuthenticator, GuestAuthenticator, etc
        if (credentials == null) {
            return new CredentialsInfo() {
                @Override
                public String getUserId() {
                    return "null";
                }

                @Override
                public String[] getAttributeNames() {
                    return new String[0];
                }

                @Override
                public Object getAttribute(String name) {
                    return null;
                }
            };
        }
        else if (credentials instanceof SimpleCredentials) {
            final SimpleCredentials c = (SimpleCredentials) credentials;
            Arrays.fill(c.getPassword(), '\0');
            return new CredentialsInfo() {
                @Override
                public String getUserId() {
                    return c.getUserID();
                }

                @Override
                public String[] getAttributeNames() {
                    return c.getAttributeNames();
                }

                @Override
                public Object getAttribute(String name) {
                    return c.getAttribute(name);
                }
            };
        }
        else if (credentials instanceof GuestCredentials) {
            return new CredentialsInfo() {
                @Override
                public String getUserId() {
                    return "anonymous";
                }

                @Override
                public String[] getAttributeNames() {
                    return new String[0]; 
                }

                @Override
                public Object getAttribute(String name) {
                    return null; 
                }
            };
        }
        else {
            throw new LoginException(format("Login failed for {0}", credentials));
        }
    }
}
