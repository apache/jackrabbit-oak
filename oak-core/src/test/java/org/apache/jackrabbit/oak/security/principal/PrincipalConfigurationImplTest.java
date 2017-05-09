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
package org.apache.jackrabbit.oak.security.principal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PrincipalConfigurationImplTest extends AbstractSecurityTest {

    private PrincipalConfigurationImpl pc1;
    private PrincipalConfigurationImpl pc2;

    @Override
    public void before() throws Exception {
        super.before();

        pc1 = new PrincipalConfigurationImpl();
        pc2 = new PrincipalConfigurationImpl(getSecurityProvider());
    }

    @Test
    public void testGetName() {
        assertEquals(PrincipalConfiguration.NAME, pc1.getName());
        assertEquals(PrincipalConfiguration.NAME, pc2.getName());
    }

    @Test
    public void testGetContext() {
        assertSame(Context.DEFAULT, pc1.getContext());
        assertSame(Context.DEFAULT, pc2.getContext());
    }

    @Test
    public void testGetParameters() {
        assertSame(ConfigurationParameters.EMPTY, pc1.getParameters());
        assertSame(ConfigurationParameters.EMPTY, pc2.getParameters());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetPrincipalManager() {
        pc1.getPrincipalManager(root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testGetPrincipalManager2() {
        pc1.setSecurityProvider(getSecurityProvider());
        PrincipalManager pm = pc1.getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertNotNull(pm);
        assertTrue(pm instanceof PrincipalManagerImpl);
    }

    @Test
    public void testGetPrincipalManager3() {
        PrincipalManager pm = pc2.getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertNotNull(pm);
        assertTrue(pm instanceof PrincipalManagerImpl);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetPrincipalProvider() {
        pc1.getPrincipalProvider(root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testGetPrincipalProvider2() {
        pc1.setSecurityProvider(getSecurityProvider());
        PrincipalProvider pp = pc1.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertNotNull(pp);
        assertEquals(getUserConfiguration().getUserPrincipalProvider(root, NamePathMapper.DEFAULT).getClass(), pp.getClass());
    }

    @Test
    public void testGetPrincipalProvider3() {
        PrincipalProvider pp = pc2.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertNotNull(pp);
        assertEquals(getUserConfiguration().getUserPrincipalProvider(root, NamePathMapper.DEFAULT).getClass(), pp.getClass());
    }

    @Test
    public void testGetPrincipalProvider4() {
        PrincipalConfigurationImpl pc3 = new PrincipalConfigurationImpl();
        final SecurityProvider sp = new SecurityProvider() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters(@Nullable String name) {
                return ConfigurationParameters.EMPTY;
            }

            @Nonnull
            @Override
            public Iterable<? extends SecurityConfiguration> getConfigurations() {
                return ImmutableList.of();
            }

            @Nonnull
            @Override
            public <T> T getConfiguration(@Nonnull Class<T> configClass) {
                if (configClass.equals(UserConfiguration.class)) {
                    return (T) new UserConfigurationImpl(this) {
                        @Nullable
                        @Override
                        public PrincipalProvider getUserPrincipalProvider(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
                            return null;
                        }
                    };
                } else {
                    throw new IllegalArgumentException();
                }
            }
        };
        pc3.setSecurityProvider(sp);

        PrincipalProvider pp = pc3.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertTrue(pp instanceof PrincipalProviderImpl);
    }
}