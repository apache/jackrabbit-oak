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

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;

/**
 * This implementation of the authentication configuration provides login
 * contexts that accept any credentials and doesn't validate specified
 * workspace name. Retrieving a {@code TokenProvider} is not supported.
 */
public class OpenAuthenticationConfiguration extends SecurityConfiguration.Default implements AuthenticationConfiguration {

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(ContentRepository contentRepository) {
        return new LoginContextProvider() {
            @Nonnull
            @Override
            public LoginContext getLoginContext(final Credentials credentials, String workspaceName) {
                return new LoginContext() {
                    @Override
                    public Subject getSubject() {
                        Subject subject = new Subject();
                        if (credentials != null) {
                            subject.getPrivateCredentials().add(credentials);
                        }
                        subject.setReadOnly();
                        return subject;
                    }
                    @Override
                    public void login() {
                        // do nothing
                    }
                    @Override
                    public void logout() {
                        // do nothing
                    }
                };
            }
        };
    }
}
