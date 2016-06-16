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
import java.security.acl.Group;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositePrincipalConfigurationTest extends AbstractSecurityTest {

    private CompositePrincipalConfiguration cpConfig = new CompositePrincipalConfiguration();

    private static void assertSize(int expected, CompositePrincipalProvider pp) throws Exception {
        Field f = CompositePrincipalProvider.class.getDeclaredField("providers");
        f.setAccessible(true);

        List<PrincipalProvider> providers = (List<PrincipalProvider>) f.get(pp);
        assertEquals(expected, providers.size());
    }

    @Test
    public void testEmptyGetPrincipalManager() {
        PrincipalManager pMgr = cpConfig.getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testEmptyGetProvider() throws Exception {
        PrincipalProvider pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertFalse(pp instanceof CompositePrincipalProvider);
        assertSame(EmptyPrincipalProvider.INSTANCE, pp);
    }

    @Test
    public void testSingleGetPrincipalManager() {
        PrincipalConfiguration testConfig = new TestPrincipalConfiguration();
        cpConfig.addConfiguration(testConfig);

        PrincipalManager pMgr = cpConfig.getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testSingleGetProvider() throws Exception {
        PrincipalConfiguration testConfig = new TestPrincipalConfiguration();
        cpConfig.addConfiguration(testConfig);

        PrincipalProvider pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);

        assertFalse(pp instanceof CompositePrincipalProvider);
        assertEquals(testConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT).getClass(), pp.getClass());
    }

    @Test
    public void testMultipleGetPrincipalManager() {
        cpConfig.addConfiguration(getSecurityProvider().getConfiguration(PrincipalConfiguration.class));
        cpConfig.addConfiguration(new TestPrincipalConfiguration());

        PrincipalManager pMgr = cpConfig.getPrincipalManager(root, NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrincipalManagerImpl);
    }

    @Test
    public void testMultipleGetPrincipalProvider() throws Exception {
        cpConfig.addConfiguration(getSecurityProvider().getConfiguration(PrincipalConfiguration.class));
        cpConfig.addConfiguration(new TestPrincipalConfiguration());

        PrincipalProvider pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);

        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);
    }

    @Test
    public void testWithEmptyPrincipalProvider() throws Exception {
        cpConfig.addConfiguration(new TestEmptyConfiguration());
        PrincipalProvider pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertSame(EmptyPrincipalProvider.INSTANCE, pp);

        cpConfig.addConfiguration(new TestPrincipalConfiguration());
        pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertFalse(pp instanceof CompositePrincipalProvider);

        cpConfig.addConfiguration(getSecurityProvider().getConfiguration(PrincipalConfiguration.class));
        pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);

        cpConfig.addConfiguration(new TestEmptyConfiguration());
        pp = cpConfig.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertTrue(pp instanceof CompositePrincipalProvider);
        assertSize(2, (CompositePrincipalProvider) pp);
    }


    private final class TestPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

        @Nonnull
        @Override
        public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
            return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
        }

        @Nonnull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return new PrincipalProvider() {
                @CheckForNull
                @Override
                public Principal getPrincipal(@Nonnull String principalName) {
                    return null;
                }

                @Nonnull
                @Override
                public Set<Group> getGroupMembership(@Nonnull Principal principal) {
                    return ImmutableSet.of();
                }

                @Nonnull
                @Override
                public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
                    return ImmutableSet.of();
                }

                @Nonnull
                @Override
                public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
                    return Iterators.emptyIterator();
                }

                @Nonnull
                @Override
                public Iterator<? extends Principal> findPrincipals(int searchType) {
                    return Iterators.emptyIterator();
                }
            };
        }

        @Nonnull
        @Override
        public String getName() {
            return PrincipalConfiguration.NAME;
        }
    }

    private final class TestEmptyConfiguration extends ConfigurationBase implements PrincipalConfiguration {

        @Nonnull
        @Override
        public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
            return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
        }

        @Nonnull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return EmptyPrincipalProvider.INSTANCE;
        }

        @Nonnull
        @Override
        public String getName() {
            return PrincipalConfiguration.NAME;
        }
    }
}