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

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModuleTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: LoginModule
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the role of {@link javax.security.auth.spi.LoginModule}s in the Oak
 * authentication setup, the way multiple login modules can be configured (both
 * in OSGi and Java based setups) and how they interact.
 *
 * Exercises:
 *
 * -
 *
 *
 * </pre>
 *
 * @see javax.security.auth.spi.LoginModule
 * @see javax.security.auth.login.Configuration
 */
public class LoginModuleTest extends AbstractSecurityTest {


    /**
     * FIXME : write this LoginModule implementation according to the needs of the corresponding exercise test
     */
    public final class TestLoginModule implements LoginModule {

        @Override
        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String,?> sharedState, Map<String,?> options) {
            // TODO

        }

        @Override
        public boolean login() throws LoginException {
            // TODO
            return false;
        }

        @Override
        public boolean commit() throws LoginException {
            // TODO
            return false;
        }

        @Override
        public boolean abort() throws LoginException {
            // TODO
            return false;
        }

        @Override
        public boolean logout() throws LoginException {
            // TODO
            return false;
        }
    }
}