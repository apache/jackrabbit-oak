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
package org.apache.jackrabbit.oak.security;

import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.junit.Before;

/**
 * AbstractOakTest is the base class for oak test execution.
 */
public abstract class AbstractSecurityTest {

    private ContentRepository contentRepository;

    @Before
    public void before() throws Exception {
        contentRepository = createRepository();
        // TODO: OAK-17. workaround for missing test configuration
        Configuration.setConfiguration(new OakConfiguration());
    }

    protected abstract ContentRepository createRepository();

    protected ContentSession login(Credentials credentials)
            throws LoginException, NoSuchWorkspaceException {
        return contentRepository.login(credentials, null);
    }

    protected Credentials getAdminCredentials() {
        // TODO retrieve from config
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

}