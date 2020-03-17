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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SecurityConfigurationTest {

    private SecurityConfiguration configuration = new SecurityConfiguration.Default();

    @Test
    public void testGetName() {
        assertEquals("org.apache.jackrabbit.oak", configuration.getName());
    }

    @Test
    public void testGetParametesr() {
        assertSame(ConfigurationParameters.EMPTY, configuration.getParameters());
    }

    @Test
    public void testGetContext() {
        assertSame(Context.DEFAULT, configuration.getContext());
    }

    @Test
    public void testGetRepositoryInitializer() {
        assertSame(RepositoryInitializer.DEFAULT, configuration.getRepositoryInitializer());
    }

    @Test
    public void testGetWorkspaceInitializer() {
        assertSame(WorkspaceInitializer.DEFAULT, configuration.getWorkspaceInitializer());
    }


    @Test
    public void testGetCommitHooks() {
        assertTrue(configuration.getCommitHooks("workspaceName").isEmpty());
    }

    @Test
    public void testGetValidators() {
        assertTrue(configuration.getValidators("workspaceName", ImmutableSet.of(), new MoveTracker()).isEmpty());
    }

    @Test
    public void testGetConflictHandlers() {
        assertTrue(configuration.getConflictHandlers().isEmpty());
    }

    @Test
    public void testGetProtectedItemImporters() {
        assertTrue(configuration.getProtectedItemImporters().isEmpty());
    }
}