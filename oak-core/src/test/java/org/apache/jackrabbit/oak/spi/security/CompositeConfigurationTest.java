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
package org.apache.jackrabbit.oak.spi.security;

import java.lang.reflect.Field;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositeConfigurationTest extends AbstractCompositeConfigurationTest {

    private static final String NAME = "test";

    @Before
    public void before() throws Exception {
        compositeConfiguration = new CompositeConfiguration("test", new SecurityProvider() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters(@Nullable String name) {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public Iterable<? extends SecurityConfiguration> getConfigurations() {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public <T> T getConfiguration(@Nonnull Class<T> configClass) {
                throw new UnsupportedOperationException();
            }
        }) {};
    }

    @Test
    public void testGetName() {
        assertEquals(NAME, compositeConfiguration.getName());
    }

    @Test
    public void testEmpty() {
        assertSame(ConfigurationParameters.EMPTY, compositeConfiguration.getParameters());
        assertTrue(getConfigurations().isEmpty());
    }

    @Test
    public void testGetDefaultConfig() {
        assertNull(compositeConfiguration.getDefaultConfig());

        SecurityConfiguration sc = new SecurityConfiguration.Default();
        setDefault(sc);

        assertSame(sc, compositeConfiguration.getDefaultConfig());
    }

    @Test
    public void testSetDefaultConfig() {
        SecurityConfiguration sc = new SecurityConfiguration.Default();
        setDefault(sc);

        List<SecurityConfiguration> configurations = getConfigurations();
        assertFalse(configurations.isEmpty());
        assertEquals(1, configurations.size());
        assertEquals(sc, configurations.iterator().next());
    }

    @Test
    public void testAddConfiguration() {
        addConfiguration(new SecurityConfiguration.Default());
        addConfiguration(new SecurityConfiguration.Default());

        List<SecurityConfiguration> configurations = getConfigurations();
        assertFalse(configurations.isEmpty());
        assertEquals(2, configurations.size());

        SecurityConfiguration def = new SecurityConfiguration.Default();
        setDefault(def);

        configurations = getConfigurations();
        assertEquals(2, configurations.size());
        assertFalse(configurations.contains(def));
    }

    @Test
    public void testAddConfigurationWithRanking() {
        SecurityConfiguration r100 = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(r100, ConfigurationParameters.of(Constants.SERVICE_RANKING, 100));

        SecurityConfiguration r200 = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(r200, ConfigurationParameters.of(Constants.SERVICE_RANKING, 200));

        SecurityConfiguration r150 = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters() {
                return ConfigurationParameters.of(CompositeConfiguration.PARAM_RANKING, 150);
            }
        };
        compositeConfiguration.addConfiguration(r150, ConfigurationParameters.EMPTY);

        SecurityConfiguration r50 = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters() {
                return ConfigurationParameters.of(CompositeConfiguration.PARAM_RANKING, 50);
            }
        };
        compositeConfiguration.addConfiguration(r50, ConfigurationParameters.EMPTY);

        SecurityConfiguration rUndef = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(rUndef, ConfigurationParameters.EMPTY);

        SecurityConfiguration r200second = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(r200second, ConfigurationParameters.of(Constants.SERVICE_RANKING, 200));

        List l = getConfigurations();
        assertArrayEquals(new SecurityConfiguration[]{r200, r200second, r150, r100, r50, rUndef}, l.toArray(new SecurityConfiguration[l.size()]));

        // remove and add new
        removeConfiguration(r150);
        removeConfiguration(r50);
        removeConfiguration(r100);

        SecurityConfiguration r75 = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(r75, ConfigurationParameters.of(Constants.SERVICE_RANKING, 75));

        l = getConfigurations();
        assertArrayEquals(new SecurityConfiguration[]{r200, r200second, r75, rUndef}, l.toArray(new SecurityConfiguration[l.size()]));
    }

    @Test
    public void testRemoveConfiguration() {
        SecurityConfiguration def = new SecurityConfiguration.Default();
        setDefault(def);

        SecurityConfiguration sc = new SecurityConfiguration.Default();
        addConfiguration(sc);

        removeConfiguration(def);
        List configurations = getConfigurations();
        assertEquals(1, configurations.size());
        assertEquals(sc, configurations.iterator().next());

        removeConfiguration(sc);
        configurations = getConfigurations();
        assertEquals(1, configurations.size());
        assertEquals(def, configurations.iterator().next());
    }

    @Test
    public void testGetContext() throws Exception {
        Class cls = Class.forName(CompositeConfiguration.class.getName() + "$CompositeContext");
        Field def = cls.getDeclaredField("defaultCtx");
        def.setAccessible(true);

        Field delegatees = cls.getDeclaredField("delegatees");
        delegatees.setAccessible(true);

        Context ctx = compositeConfiguration.getContext();
        assertSame(cls, ctx.getClass());
        assertNull(delegatees.get(ctx));
        assertSame(Context.DEFAULT, def.get(ctx));

        SecurityConfiguration sc = new TestConfiguration();
        setDefault(sc);
        ctx = compositeConfiguration.getContext();
        assertNull(delegatees.get(ctx));
        assertSame(sc.getContext(), def.get(ctx));
        assertSame(cls, ctx.getClass());

        addConfiguration(sc);
        ctx = compositeConfiguration.getContext();
        assertNotSame(sc.getContext(), ctx);
        assertEquals(1, ((Context[]) delegatees.get(ctx)).length);

        // add configuration that has DEFAULT ctx -> must not be added
        SecurityConfiguration defConfig = new SecurityConfiguration.Default();
        addConfiguration(defConfig);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        // add same test configuration again -> no duplicate entries
        addConfiguration(sc);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        SecurityConfiguration sc2 = new TestConfiguration();
        addConfiguration(sc2);
        assertEquals(2, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        removeConfiguration(sc2);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        removeConfiguration(sc);
        removeConfiguration(sc);
        removeConfiguration(defConfig);
        assertNull(delegatees.get(compositeConfiguration.getContext()));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSecurityProviderNotInitialized() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};
        cc.getSecurityProvider();
    }

    @Test()
    public void testSetSecurityProvider() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};

        SecurityProvider securityProvider = Mockito.mock(SecurityProvider.class);
        cc.setSecurityProvider(securityProvider);

        assertSame(securityProvider, cc.getSecurityProvider());
    }

    @Test
    public void testGetProtectedItemImporter() {
        assertTrue(compositeConfiguration.getProtectedItemImporters().isEmpty());

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getProtectedItemImporters().isEmpty());

        SecurityConfiguration withImporter = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public List<ProtectedItemImporter> getProtectedItemImporters() {
                return ImmutableList.of(Mockito.mock(ProtectedItemImporter.class));
            }
        };
        addConfiguration(withImporter);

        assertEquals(1, compositeConfiguration.getProtectedItemImporters().size());
    }

    private static final class TestConfiguration extends SecurityConfiguration.Default {

        private final Context ctx = new TestContext();
        @Nonnull
        @Override
        public Context getContext() {
            return ctx;
        }
    }

    private static final class TestContext extends Context.Default {

    }
}