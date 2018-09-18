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
package org.apache.jackrabbit.oak.exercise.security.authentication;

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom login module for test purposes.
 *
 * EXERCISE: complete the implemenation
 */
public class CustomLoginModule implements LoginModule {

    private static final Logger log = LoggerFactory.getLogger(CustomLoginModule.class);

    private ConfigurationParameters config;

    public CustomLoginModule() {
        this(ConfigurationParameters.EMPTY);
    }

    public CustomLoginModule(ConfigurationParameters config) {
        this.config = config;
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        if (options != null) {
            ConfigurationParameters opts = ConfigurationParameters.of(options);
            config = (config == null) ? opts : ConfigurationParameters.of(config, opts);
        }

        // EXERCISE
    }

    @Override
    public boolean login() throws LoginException {
        // EXERCISE
        return false;
    }

    @Override
    public boolean commit() throws LoginException {
        // EXERCISE
        return false;
    }

    @Override
    public boolean abort() throws LoginException {
        // EXERCISE
        return false;
    }

    @Override
    public boolean logout() throws LoginException {
        // EXERCISE
        return false;
    }
}