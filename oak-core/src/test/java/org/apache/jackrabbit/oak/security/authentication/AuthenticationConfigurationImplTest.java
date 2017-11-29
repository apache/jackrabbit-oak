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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class AuthenticationConfigurationImplTest {

    private final AuthenticationConfigurationImpl authConfiguration = new AuthenticationConfigurationImpl();
    private final ContentRepository repo = Mockito.mock(ContentRepository.class);

    @Test
    public void testGetName() {
        assertEquals(AuthenticationConfiguration.NAME, authConfiguration.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetLoginCtxProviderNotInitialized() {
       authConfiguration.getLoginContextProvider(repo);
    }

    @Test
    public void testGetLoginCtxProvider() {
        authConfiguration.setSecurityProvider(new SecurityProviderBuilder().build());

        assertNotNull(authConfiguration.getLoginContextProvider(repo));
    }

    @Test
    public void testGetLoginCtxProviderWhiteboard() {
        SecurityProvider sp = Mockito.mock(SecurityProvider.class, Mockito.withSettings().extraInterfaces(WhiteboardAware.class));
        when(((WhiteboardAware) sp).getWhiteboard()).thenReturn(new DefaultWhiteboard());

        authConfiguration.setSecurityProvider(sp);

        assertNotNull(authConfiguration.getLoginContextProvider(repo));
    }
}