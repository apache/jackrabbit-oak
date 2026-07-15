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
package org.apache.jackrabbit.oak.security.authentication.ldap;

import java.util.Collections;
import javax.jcr.GuestCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * LDAP Login should not only work in the default jaas configuration
 *
 * LoginModuleImpl SUFFICIENT
 * ExternalLoginModule REQUIRED
 *
 * but also with the jaas configuration specifying the external login module
 * first:
 *
 * ExternalLoginModule SUFFICIENT
 * LoginModuleImpl REQUIRED
 */
public class LdapDefaultLoginModuleTest extends LdapLoginTestBase {

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                                options),
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                Collections.<String, Object>emptyMap())
                };
            }
        };
    }

    /**
     * Login with {@link javax.jcr.GuestCredentials} must succeed and result in
     * an guest session as the SUFFICIENT
     * {@link org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl}
     * handles the guest login (in contrast to the ExternalLoginModule).
     *
     * @throws Exception
     */
    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = login(new GuestCredentials());
        assertEquals(UserConstants.DEFAULT_ANONYMOUS_ID, cs.getAuthInfo().getUserID());
        cs.close();
    }
}
