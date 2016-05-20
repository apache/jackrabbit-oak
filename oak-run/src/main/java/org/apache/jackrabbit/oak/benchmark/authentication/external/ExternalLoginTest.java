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
package org.apache.jackrabbit.oak.benchmark.authentication.external;

import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;

/**
 * Login against the {@link ExternalLoginModule} with a randomly selected user.
 * The first login of a given user will trigger the user-synchronization mechanism.
 * Subsequent login calls will only result in an extra sync-call if the configured
 * expiration time is reached.
 *
 * Configuration options as defined in {@link AbstractExternalTest}.
 */
public class ExternalLoginTest extends AbstractExternalTest {

    public ExternalLoginTest(int numberOfUsers, int numberOfGroups, long expTime,
                             boolean dynamicMembership, @Nonnull List<String> autoMembership) {
        super(numberOfUsers, numberOfGroups, expTime, dynamicMembership, autoMembership);
    }

    @Override
    protected void runTest() throws Exception {
        getRepository().login(new SimpleCredentials(getRandomUserId(), new char[0])).logout();
    }

    protected Configuration createConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                                ImmutableMap.<String, Object>of()),
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                ImmutableMap.of(
                                        ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, syncConfig.getName(),
                                        ExternalLoginModule.PARAM_IDP_NAME, idp.getName()))
                };
            }
        };
    }
}