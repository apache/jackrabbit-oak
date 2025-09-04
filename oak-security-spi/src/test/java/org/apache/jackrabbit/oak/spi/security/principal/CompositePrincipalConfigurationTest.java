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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.AbstractCompositeConfigurationTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CompositePrincipalConfigurationTest extends AbstractCompositeConfigurationTest<PrincipalConfiguration> {

    private final Root root = mock(Root.class);

    private PrincipalConfiguration principalConfigurationMock;

    @Before
    public void before() {
        compositeConfiguration = new CompositePrincipalConfiguration();
        principalConfigurationMock = mock(PrincipalConfiguration.class);
        Mockito.when(principalConfigurationMock.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
    }

    private static void assertSize(int expected, CompositePrincipalProvider pp) throws Exception {
        Field f = CompositePrincipalProvider.class.getDeclaredField("providers");
        f.setAccessible(true);

        List<PrincipalProvider> providers = (List<PrincipalProvider>) f.get(pp);
        assertEquals(expected, providers.size());
    }

    @Test
    public void testEmptyGetPrincipalManager() {
        PrincipalManager pMgr = getComposite().getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testEmptyGetProvider() {
        PrincipalProvider pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertFalse(pp instanceof CompositePrincipalProvider);
        assertSame(EmptyPrincipalProvider.INSTANCE, pp);
    }

    @Test
    public void testSingleGetPrincipalManager() {
        PrincipalConfiguration testConfig = new TestPrincipalConfiguration();
        addConfiguration(testConfig);

        PrincipalManager pMgr = getComposite().getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testSingleGetProvider() {
        PrincipalConfiguration testConfig = new TestPrincipalConfiguration();
        addConfiguration(testConfig);

        PrincipalProvider pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);

        assertFalse(pp instanceof CompositePrincipalProvider);
        assertEquals(testConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT).getClass(), pp.getClass());
    }

    @Test
    public void testMultipleGetPrincipalManager() {
        addConfiguration(principalConfigurationMock);
        addConfiguration(new TestPrincipalConfiguration());

        PrincipalManager pMgr = getComposite().getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testMultipleGetPrincipalProvider() throws Exception {
        addConfiguration(principalConfigurationMock);
        addConfiguration(new TestPrincipalConfiguration());

        PrincipalProvider pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);

        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);
    }

    @Test
    public void testWithEmptyPrincipalProvider() throws Exception {
        addConfiguration(new TestEmptyConfiguration());
        PrincipalProvider pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertSame(EmptyPrincipalProvider.INSTANCE, pp);

        addConfiguration(new TestPrincipalConfiguration());
        pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertFalse(pp instanceof CompositePrincipalProvider);

        addConfiguration(principalConfigurationMock);
        pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);

        addConfiguration(new TestEmptyConfiguration());
        pp = getComposite().getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);
    }

    @Test
    public void testInitWithSecurityProvider() {
        SecurityProvider sp = mock(SecurityProvider.class);
        TestComposite cpc = new TestComposite(sp);

        assertSame(PrincipalConfiguration.NAME, cpc.getName());
        assertSame(sp, cpc.getSecurityProvider());
    }

    private static final class TestComposite extends CompositePrincipalConfiguration {
        TestComposite(@NotNull SecurityProvider securityProvider) {
            super(securityProvider);
        }
        @NotNull
        public SecurityProvider getSecurityProvider() {
            return super.getSecurityProvider();
        }
    }

    private static final class TestPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

        @NotNull
        @Override
        public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
            return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
        }

        @NotNull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return new TestPrincipalProvider();
        }

        @NotNull
        @Override
        public String getName() {
            return PrincipalConfiguration.NAME;
        }
    }

    private static class TestPrincipalProvider implements PrincipalProvider {

        @Nullable
        @Override
        public Principal getPrincipal(@NotNull String principalName) {
            return null;
        }

        @NotNull
        @Override
        public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
            return Set.of();
        }

        @NotNull
        @Override
        public Set<? extends Principal> getPrincipals(@NotNull String userID) {
            return Set.of();
        }

        @NotNull
        @Override
        public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
            return Collections.emptyIterator();
        }

        @NotNull
        @Override
        public Iterator<? extends Principal> findPrincipals(int searchType) {
            return Collections.emptyIterator();
        }
    }

    private static final class TestEmptyConfiguration extends ConfigurationBase implements PrincipalConfiguration {

        @NotNull
        @Override
        public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
            return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
        }

        @NotNull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return EmptyPrincipalProvider.INSTANCE;
        }

        @NotNull
        @Override
        public String getName() {
            return PrincipalConfiguration.NAME;
        }
    }
}
