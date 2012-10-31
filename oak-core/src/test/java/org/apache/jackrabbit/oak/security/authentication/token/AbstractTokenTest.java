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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.After;
import org.junit.Before;

/**
 * AbstractTokenTest...
 */
public abstract class AbstractTokenTest extends AbstractSecurityTest {

    Root root;
    TokenProviderImpl tokenProvider;

    String userId;
    UserManager userManager;

    @Before
    public void before() throws Exception {
        super.before();

        root = admin.getLatestRoot();
        tokenProvider = new TokenProviderImpl(root,
                ConfigurationParameters.EMPTY,
                getSecurityProvider().getUserConfiguration());

        userId = "testUser";
        userManager = getSecurityProvider().getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);

        userManager.createUser(userId, "pw");
        root.commit();
    }

    @After
    public void after() throws Exception {
        try {
            Authorizable a = userManager.getAuthorizable(userId);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }
}