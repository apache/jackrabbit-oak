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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.security.authentication.monitor.LoginModuleMonitorImpl;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationConfigurationImplTest {

    private final AuthenticationConfigurationImpl authConfiguration = new AuthenticationConfigurationImpl();
    private final ContentRepository repo = mock(ContentRepository.class);

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
        authConfiguration.setSecurityProvider(SecurityProviderBuilder.newBuilder().build());

        assertNotNull(authConfiguration.getLoginContextProvider(repo));
    }

    @Test
    public void testGetLoginCtxProviderWhiteboard() throws Exception {
        Whiteboard wb = new DefaultWhiteboard();
        SecurityProvider sp = mock(SecurityProvider.class, Mockito.withSettings().extraInterfaces(WhiteboardAware.class));
        when(((WhiteboardAware) sp).getWhiteboard()).thenReturn(wb);
        authConfiguration.setSecurityProvider(sp);

        LoginContextProvider lcp = authConfiguration.getLoginContextProvider(repo);
        assertTrue(lcp instanceof LoginContextProviderImpl);

        Field f = LoginContextProviderImpl.class.getDeclaredField("whiteboard");
        f.setAccessible(true);
        assertSame(wb, f.get(lcp));
    }

    @Test
    public void testGetLoginCtxProviderWithoutWhiteboard() throws Exception {
        SecurityProvider sp = mock(SecurityProvider.class);
        authConfiguration.setSecurityProvider(sp);

        LoginContextProvider lcp = authConfiguration.getLoginContextProvider(repo);
        assertTrue(lcp instanceof LoginContextProviderImpl);

        Field f = LoginContextProviderImpl.class.getDeclaredField("whiteboard");
        f.setAccessible(true);
        assertNull(f.get(lcp));
    }

    @Test
    public void testGetMonitors() throws Exception {
        Field f = AuthenticationConfigurationImpl.class.getDeclaredField("lmMonitor");
        f.setAccessible(true);
        assertSame(LoginModuleMonitor.NOOP, f.get(authConfiguration));

        StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        Iterable<Monitor<?>> monitors = authConfiguration.getMonitors(statisticsProvider);
        assertEquals(1, Iterables.size(monitors));

        Monitor<?> m = monitors.iterator().next();
        assertTrue(m instanceof LoginModuleMonitorImpl);
        assertTrue(f.get(authConfiguration) instanceof LoginModuleMonitorImpl);
    }
}