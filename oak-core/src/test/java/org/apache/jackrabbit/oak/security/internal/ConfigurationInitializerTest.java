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
package org.apache.jackrabbit.oak.security.internal;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ConfigurationInitializerTest {

    private final SecurityProvider sp = new InternalSecurityProvider();
    private final ConfigurationParameters params = ConfigurationParameters.of("key", "value");

    private final RootProvider rootProvider = Mockito.mock(RootProvider.class);
    private final TreeProvider treeProvider = Mockito.mock(TreeProvider.class);

    @Test
    public void testInitConfigurationReturnsSame() {
        SecurityConfiguration sc = new SecurityConfiguration.Default();

        assertSame(sc, ConfigurationInitializer.initializeConfiguration(sc, sp, rootProvider, treeProvider));
    }

    @Test
    public void testInitBaseConfigurationReturnsSame() {
        SecurityConfiguration sc = new TestConfiguration();

        assertSame(sc, ConfigurationInitializer.initializeConfiguration(sc, sp, rootProvider, treeProvider));
    }

    @Test
    public void testInitConfigurationWithParamReturnsSame() {
        SecurityConfiguration sc = new SecurityConfiguration.Default();
        assertSame(sc, ConfigurationInitializer.initializeConfiguration(sc, sp, params, rootProvider, treeProvider));
    }

    @Test
    public void testInitBaseConfigurationWithParamReturnsSame() {
        SecurityConfiguration sc = new TestConfiguration();
        assertSame(sc, ConfigurationInitializer.initializeConfiguration(sc, sp, params, rootProvider, treeProvider));
    }

    @Test
    public void testInitNonBaseConfiguration() {
        SecurityConfiguration sc = new SecurityConfiguration.Default();

        ConfigurationInitializer.initializeConfiguration(sc, sp, rootProvider, treeProvider);
        assertFalse(sc.getParameters().containsKey("key"));
    }

    @Test
    public void testInitBaseConfiguration() {
        TestConfiguration sc = new TestConfiguration();

        SecurityConfiguration afterInit = ConfigurationInitializer.initializeConfiguration(sc, sp, rootProvider, treeProvider);
        assertSame(sc, afterInit);

        // verify securityprovider
        assertSame(sp, sc.getSecurityProvider());

        // verify params
        ConfigurationParameters parameters = afterInit.getParameters();
        assertTrue(parameters.containsKey("key"));
        assertTrue(parameters.containsKey("key2"));
        assertEquals("initialValue", parameters.get("key"));
        assertEquals("initialValue", parameters.get("key2"));
    }

    @Test
    public void testInitBaseConfigurationWithParam() {
        TestConfiguration sc = new TestConfiguration();

        SecurityConfiguration afterInit = ConfigurationInitializer.initializeConfiguration(sc, sp, params, rootProvider, treeProvider);
        assertSame(sc, afterInit);

        // verify securityprovider
        assertSame(sp, sc.getSecurityProvider());

        // verify tree/root provider
        assertSame(rootProvider, sc.getRootProvider());
        assertSame(treeProvider, sc.getTreeProvider());

        // verify params
        ConfigurationParameters parameters = afterInit.getParameters();
        assertTrue(parameters.containsKey("key"));
        assertTrue(parameters.containsKey("key2"));
        assertEquals("value", parameters.get("key"));
        assertEquals("initialValue", parameters.get("key2"));
    }

    @Test
    public void testInitCompositeConfiguration() {
        TestComposite<SecurityConfiguration.Default> composite = new TestComposite<SecurityConfiguration.Default>();
        composite.addConfiguration(new SecurityConfiguration.Default());
        composite.addConfiguration(new SecurityConfiguration.Default());

        ConfigurationInitializer.initializeConfigurations(composite, sp, params, rootProvider, treeProvider);

        // verify securityprovider
        assertSame(sp, composite.getSecurityProvider());

        // verify params
        for (SecurityConfiguration.Default sc : composite.getConfigurations()) {
            assertFalse(sc.getParameters().containsKey("key"));
        }
    }

    @Test
    public void testInitCompositeBaseConfiguration() {
        TestComposite<TestConfiguration> composite = new TestComposite<TestConfiguration>();
        composite.addConfiguration(new TestConfiguration());
        composite.addConfiguration(new TestConfiguration());

        ConfigurationInitializer.initializeConfigurations(composite, sp, params, rootProvider, treeProvider);

        // verify securityprovider
        assertSame(sp, composite.getSecurityProvider());

        // verify params
        for (SecurityConfiguration sc : composite.getConfigurations()) {
            ConfigurationParameters parameters = sc.getParameters();
            assertTrue(parameters.containsKey("key"));
            assertTrue(parameters.containsKey("key2"));
            assertEquals("value", parameters.get("key"));
            assertEquals("initialValue", parameters.get("key2"));
        }
    }

    private final class TestConfiguration extends ConfigurationBase {

        TestConfiguration() {
            super(sp, ConfigurationParameters.of("key", "initialValue", "key2", "initialValue"));
        }
    }

    private final class TestComposite<T extends SecurityConfiguration> extends CompositeConfiguration<T> {

        public TestComposite() {
            super("name");
        }

        @Nonnull
        @Override
        public SecurityProvider getSecurityProvider() {
            return super.getSecurityProvider();
        }
    }
}