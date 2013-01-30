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

import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * JackrabbitTest... TODO
 */
public class Jackrabbit2ConfigurationTest extends TokenDefaultLoginModuleTest {

    @Override
    protected Configuration getConfiguration() {
        return ConfigurationUtil.getJackrabbit2Configuration(ConfigurationParameters.EMPTY);
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = login(null);
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtility.getAnonymousId(getUserConfiguration().getConfigurationParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }
}