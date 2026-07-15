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

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import static org.junit.Assert.assertEquals;

public class ConfigurationUtilTest {

    @Test
    public void testGetDefaultConfiguration() {
        Configuration c = ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
        AppConfigurationEntry[]  entries = c.getAppConfigurationEntry("any");
        assertEquals(1, entries.length);
        AppConfigurationEntry entry = entries[0];
        assertEquals("org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl", entry.getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, entry.getControlFlag());
    }

    @Test
    public void testGetJr2Configuration() {
        Configuration c = ConfigurationUtil.getJackrabbit2Configuration(ConfigurationParameters.EMPTY);
        AppConfigurationEntry[]  entries = c.getAppConfigurationEntry("any");
        assertEquals(3, entries.length);

        assertEquals(GuestLoginModule.class.getName(), entries[0].getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, entries[0].getControlFlag());

        assertEquals("org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule", entries[1].getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT, entries[1].getControlFlag());

        assertEquals("org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl", entries[2].getLoginModuleName());
    }
}