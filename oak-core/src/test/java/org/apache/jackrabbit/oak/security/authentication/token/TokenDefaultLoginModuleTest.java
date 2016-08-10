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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.Collections;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.Jackrabbit2ConfigurationTest;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test login behavior with the following configuration:
 *
 * <pre>
 *     jackrabbit.oak {
 *            org.apache.jackrabbit.oak.spi.security.authentication.token.TokenLoginModule sufficient;
 *            org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
 *     };
 * </pre>
 */
public class TokenDefaultLoginModuleTest extends Jackrabbit2ConfigurationTest {

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry tokenEntry = new AppConfigurationEntry(
                        TokenLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                        Collections.<String, Object>emptyMap());

                AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                        LoginModuleImpl.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Collections.<String, Object>emptyMap());
                return new AppConfigurationEntry[] {tokenEntry, defaultEntry};
            }
        };
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }
}