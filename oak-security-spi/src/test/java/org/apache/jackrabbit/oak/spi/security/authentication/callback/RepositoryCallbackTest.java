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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class RepositoryCallbackTest {

    private final RepositoryCallback cb = new RepositoryCallback();
    
    @Test
    public void testSetContentRepository() {
        assertNull(cb.getContentRepository());
        ContentRepository repo = mock(ContentRepository.class);
        cb.setContentRepository(repo);
        assertSame(repo, cb.getContentRepository());
        verifyNoInteractions(repo);
    }

    @Test
    public void testSetSecurityProvider() {
        assertNull(cb.getSecurityProvider());
        SecurityProvider sp = mock(SecurityProvider.class);
        cb.setSecurityProvider(sp);
        assertSame(sp, cb.getSecurityProvider());
        verifyNoInteractions(sp);
    }

    @Test
    public void testSetWorkspaceName() {
        assertNull(cb.getWorkspaceName());
        String wspName = "wsp";
        cb.setWorkspaceName(wspName);
        assertSame(wspName, cb.getWorkspaceName());
    }

    @Test
    public void testSetLoginModuleMonitor() {
        assertNull(cb.getLoginModuleMonitor());
        LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
        cb.setLoginModuleMonitor(monitor);
        assertSame(monitor, cb.getLoginModuleMonitor());
        verifyNoInteractions(monitor);
    }

}