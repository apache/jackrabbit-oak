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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.security.principal.KernelPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

import javax.jcr.Credentials;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * LoginContextProviderImpl...
 */
public class LoginContextProviderImpl implements LoginContextProvider {

    private static final String APP_NAME = "jackrabbit.oak";

    private final Configuration authConfig;
    private final PrincipalProvider principalProvider;

    public LoginContextProviderImpl(ContentRepository repository) {
        // TODO: use configurable authentication config and principal provider
        authConfig = new ConfigurationImpl();
        principalProvider = new KernelPrincipalProvider();
    }

    @Override
    public LoginContext getLoginContext(Credentials credentials, String workspaceName) throws LoginException {
        // TODO: add proper implementation
        // TODO  - authentication against configurable spi-authentication
        // TODO  - validation of workspace name (including access rights for the given 'user')
        return new LoginContext(APP_NAME, null, new CallbackHandlerImpl(credentials, principalProvider), authConfig);
    }
}