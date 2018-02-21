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
package org.apache.jackrabbit.oak.security.privilege;

import java.security.Principal;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PrivilegeConfigurationImplTest {

    private final PrivilegeConfigurationImpl configuration = new PrivilegeConfigurationImpl();

    @Before
    public void before() {
        configuration.setRootProvider(new RootProviderService());
    }

    @Test
    public void testGetName() {
        assertEquals(PrivilegeConfiguration.NAME, configuration.getName());
    }

    @Test
    public void testGetPrivilegeManager() {
        PrivilegeManager pMgr = configuration.getPrivilegeManager(Mockito.mock(Root.class), NamePathMapper.DEFAULT);
        assertTrue(pMgr instanceof PrivilegeManagerImpl);
    }

    @Test
    public void testGetRepositoryInitializer() {
        assertTrue(configuration.getRepositoryInitializer() instanceof PrivilegeInitializer);
    }

    @Test
    public void testGetCommitHooks() {
        List<? extends CommitHook> l = configuration.getCommitHooks("wspName");
        assertEquals(1, l.size());
        assertTrue(l.get(0) instanceof JcrAllCommitHook);
    }

    @Test
    public void testGetValidators() {
        List<? extends ValidatorProvider> l = configuration.getValidators("wspName", ImmutableSet.<Principal>of(), new MoveTracker());
        assertEquals(1, l.size());
        assertTrue(l.get(0) instanceof PrivilegeValidatorProvider);
    }

    @Test
    public void testGetContext() {
        assertSame(PrivilegeContext.getInstance(), configuration.getContext());
    }
}