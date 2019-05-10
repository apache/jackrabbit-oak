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
package org.apache.jackrabbit.oak.security.authorization.permission;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VersionablePathHookTest extends AbstractSecurityTest  {

    private ProviderCtx ctx = mock(ProviderCtx.class);
    private VersionablePathHook vpHook;

    private Tree t;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        t = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        vpHook = new VersionablePathHook(root.getContentSession().getWorkspaceName(), ctx);

        when(ctx.getRootProvider()).thenReturn(getRootProvider());
        when(ctx.getTreeProvider()).thenReturn(getTreeProvider());
    }

    @Test
    public void testInvalidVersionHistoryIsIgnored() throws Exception {
        NodeState after = new AbstractNodeState() {
            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public @NotNull Iterable<? extends PropertyState> getProperties() {
                return ImmutableList.of(PropertyStates.createProperty(JCR_VERSIONHISTORY, "someValue"));
            }

            @Override
            public boolean hasChildNode(@NotNull String name) {
                return true;
            }

            @Override
            public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
                return this;
            }

            @Override
            public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return Collections.EMPTY_LIST;
            }

            @Override
            public @NotNull NodeBuilder builder() {
                return new MemoryNodeBuilder(this);
            }
        };

        NodeState spyAfter = spy(after);
        NodeState before = getTreeProvider().asNodeState(t);

        vpHook.processCommit(before, spyAfter, new CommitInfo("sid", null));

        // never enters ReadOnlyVersionManager.getOrCreateVersionHistory that first looks up uuid
        verify(spyAfter, never()).getProperty(JCR_UUID);
    }

    @Test
    public void testToString() {
        VersionablePathHook h1 = new VersionablePathHook("anyWspName", ctx);
        VersionablePathHook h2 = new VersionablePathHook("anotherWspName", ctx);
        assertNotEquals(h1.toString(), h2.toString());
        assertEquals(h1.toString(), new VersionablePathHook("anyWspName", mock(ProviderCtx.class)).toString());
    }
}