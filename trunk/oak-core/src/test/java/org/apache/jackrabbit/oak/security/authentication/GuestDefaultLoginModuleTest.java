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

import java.util.Collections;
import javax.jcr.GuestCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test the following login configuration:
 *
 * <pre>
 * jackrabbit.oak {
 *            org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule  optional;
 *            org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
 * };
 * </pre>
 */
public class GuestDefaultLoginModuleTest extends AbstractSecurityTest {

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry guestEntry = new AppConfigurationEntry(
                        GuestLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        Collections.<String, Object>emptyMap());

                AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                        LoginModuleImpl.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Collections.<String, Object>emptyMap());

                return new AppConfigurationEntry[] {guestEntry, defaultEntry};
            }
        };
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = login(null);
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = login(new GuestCredentials());
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }
}