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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_PRIVILEGES;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrivilegeInitializerTest extends AbstractSecurityTest {

    private PrivilegeInitializer initializer;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        initializer = new PrivilegeInitializer(getRootProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingJcrSystem() {
        initializer.initialize(mock(NodeBuilder.class));
    }

    @Test
    public void testAlreadyInitialized() {
        NodeBuilder nb = when(mock(NodeBuilder.class).hasChildNode(JCR_SYSTEM)).thenReturn(true).getMock();
        when(nb.getChildNode(JCR_SYSTEM)).thenReturn(nb);
        when(nb.hasChildNode(REP_PRIVILEGES)).thenReturn(true);

        initializer.initialize(nb);

        verify(nb, never()).child(anyString());
    }

    @Test(expected = RuntimeException.class)
    public void testPrivilegeRegistrationFails() {
        try {
            NodeState ns = mock(NodeState.class);
            NodeBuilder nb = when(mock(NodeBuilder.class).hasChildNode(JCR_SYSTEM)).thenReturn(true).getMock();
            when(nb.getChildNode(anyString())).thenReturn(nb);
            when(nb.child(anyString())).thenReturn(nb);
            when(nb.getNodeState()).thenReturn(ns);

            Tree t = when(mock(Tree.class).exists()).thenReturn(true).getMock();
            when(t.hasChild(anyString())).thenReturn(true);

            Root r = when(mock(Root.class).getTree(anyString())).thenReturn(t).getMock();
            RootProvider rp = when(mock(RootProvider.class).createSystemRoot(any(NodeStore.class), isNull())).thenReturn(r).getMock();
            PrivilegeInitializer pi = new PrivilegeInitializer(rp);
            pi.initialize(nb);
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof RepositoryException);
            throw e;
        }
    }
}