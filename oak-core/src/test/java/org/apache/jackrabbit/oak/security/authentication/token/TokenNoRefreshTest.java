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
import java.util.Date;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TokenNoRefreshTest extends AbstractTokenTest {

    private String userId;

    @Override
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @Override
    ConfigurationParameters getTokenConfig() {
        return ConfigurationParameters.of(TokenProvider.PARAM_TOKEN_REFRESH, false);
    }

    @Test
    public void testNotReset() {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        assertNotNull(info);
        assertFalse(info.resetExpiration(new Date().getTime()));

        long loginTime = new Date().getTime() + 3600000;
        assertFalse(info.resetExpiration(loginTime));
    }
}
