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

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeWorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    @Test(expected = IllegalStateException.class)
    public void testGetRootProviderNotInitialized() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};
        cc.getRootProvider();
    }

    @Test()
    public void testSetRootProvider() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};

        RootProvider rootProvider = Mockito.mock(RootProvider.class);
        cc.setRootProvider(rootProvider);

        assertSame(rootProvider, cc.getRootProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTreeProviderNotInitialized() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};
        cc.getTreeProvider();
    }

    @Test()
    public void testSetTreeProvider() {
        CompositeConfiguration cc = new CompositeConfiguration("name") {};

        TreeProvider treeProvider = Mockito.mock(TreeProvider.class);
        cc.setTreeProvider(treeProvider);

        assertSame(treeProvider, cc.getTreeProvider());
    }

    @Test
    public void testGetProtectedItemImporters() {
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

    @Test
    public void testGetConflictHandlers() {
        assertTrue(compositeConfiguration.getConflictHandlers().isEmpty());

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getConflictHandlers().isEmpty());

        SecurityConfiguration withConflictHandler = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public List<ThreeWayConflictHandler> getConflictHandlers() {
                return ImmutableList.of(Mockito.mock(ThreeWayConflictHandler.class));
            }
        };
        addConfiguration(withConflictHandler);

        assertEquals(1, compositeConfiguration.getConflictHandlers().size());
    }

    @Test
    public void testGetCommitHooks() {
        assertTrue(compositeConfiguration.getCommitHooks(null).isEmpty());

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getCommitHooks(null).isEmpty());

        SecurityConfiguration withCommitHook = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName) {
                return ImmutableList.of(Mockito.mock(CommitHook.class));
            }
        };
        addConfiguration(withCommitHook);

        assertEquals(1, compositeConfiguration.getCommitHooks(null).size());
    }

    @Test
    public void testGetValidators() {
        assertTrue(compositeConfiguration.getValidators(null, ImmutableSet.of(), new MoveTracker()).isEmpty());

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getValidators(null, ImmutableSet.of(), new MoveTracker()).isEmpty());

        SecurityConfiguration withValidator = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
                return ImmutableList.of(Mockito.mock(ValidatorProvider.class));
            }
        };
        addConfiguration(withValidator);

        assertEquals(1, compositeConfiguration.getValidators(null, ImmutableSet.of(), new MoveTracker()).size());
    }

    @Test
    public void testGetWorkspaceInitializer() {
        assertTrue(compositeConfiguration.getWorkspaceInitializer() instanceof CompositeWorkspaceInitializer);

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getWorkspaceInitializer() instanceof CompositeWorkspaceInitializer);

        SecurityConfiguration withWorkspaceInitializer = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public WorkspaceInitializer getWorkspaceInitializer() {
                return Mockito.mock(WorkspaceInitializer.class);
            }
        };
        addConfiguration(withWorkspaceInitializer);

        assertTrue(compositeConfiguration.getWorkspaceInitializer() instanceof CompositeWorkspaceInitializer);
    }

    @Test
    public void testGetRepositoryInitializer() {
        assertTrue(compositeConfiguration.getRepositoryInitializer() instanceof CompositeInitializer);

        addConfiguration(new SecurityConfiguration.Default());
        assertTrue(compositeConfiguration.getRepositoryInitializer() instanceof CompositeInitializer);

        SecurityConfiguration withRepositoryInitializer = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public RepositoryInitializer getRepositoryInitializer() {
                return Mockito.mock(RepositoryInitializer.class);
            }
        };
        addConfiguration(withRepositoryInitializer);

        assertTrue(compositeConfiguration.getRepositoryInitializer() instanceof CompositeInitializer);
    }

    @Test
    public void testGetParameters() {
        assertSame(ConfigurationParameters.EMPTY, compositeConfiguration.getParameters());

        addConfiguration(new SecurityConfiguration.Default());
        assertSame(ConfigurationParameters.EMPTY, compositeConfiguration.getParameters());

        ConfigurationParameters params = ConfigurationParameters.of("a", "valueA", "b", "valueB");
        SecurityConfiguration withParams = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters() {
                return params;
            }
        };
        addConfiguration(withParams);

        assertEquals(ImmutableSet.copyOf(params.keySet()), ImmutableSet.copyOf(compositeConfiguration.getParameters().keySet()));

        ConfigurationParameters params2 = ConfigurationParameters.of("a", "valueA2", "c", "valueC");
        SecurityConfiguration withParams2 = new SecurityConfiguration.Default() {
            @Nonnull
            @Override
            public ConfigurationParameters getParameters() {
                return params2;
            }
        };
        addConfiguration(withParams2);

        ConfigurationParameters compositeParams = compositeConfiguration.getParameters();
        assertEquals(3, compositeParams.size());
        assertEquals(ImmutableSet.copyOf(ConfigurationParameters.of(params, params2).keySet()), ImmutableSet.copyOf(compositeParams.keySet()));
        assertEquals("valueA2", compositeParams.getConfigValue("a", "def"));
    }
}