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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ImmutableRootTest extends AbstractSecurityTest {

    private ImmutableRoot root;

    @Before
    public void before() throws Exception {
        super.before();

        // Add test content
        Root root = adminSession.getLatestRoot();
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        Tree x = TreeUtil.addChild(tree, "x", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree y = TreeUtil.addChild(x, "y", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree z = TreeUtil.addChild(y, "z", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        this.root = new ImmutableRoot(adminSession.getLatestRoot());
    }

    @Test
    public void testHasPendingChanges() {
        assertFalse(root.hasPendingChanges());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCommit() {
        root.commit();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCommitWithMap() {
        root.commit(Map.of());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRebase() {
        root.rebase();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRefresh() {
        root.refresh();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateBlob() {
        root.createBlob(new ByteArrayInputStream(new byte[0]));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMove() {
        root.move("/x", "/b");
    }

    @Test
    public void testGetBlob() {
        assertNull(root.getBlob("reference"));
    }

    @Test
    public void testGetContentSession() {
        ContentSession cs = root.getContentSession();
        assertNotEquals(adminSession, cs);
        assertSame(adminSession.getAuthInfo(), cs.getAuthInfo());
        assertEquals(adminSession.getWorkspaceName(), cs.getWorkspaceName());
        assertSame(root, cs.getLatestRoot());
    }

    @Test
    public void testGetContentSessionCreatedFromImmutableRoot() {
        ImmutableRoot ir = new ImmutableRoot(root);
        ContentSession cs = ir.getContentSession();
        assertSame(root.getContentSession().getAuthInfo(), cs.getAuthInfo());
        assertEquals(root.getContentSession().getWorkspaceName(), cs.getWorkspaceName());
        try {
            cs.close();
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testGetContentSessionCreatedFromRootTree() {
        ImmutableRoot ir = new ImmutableRoot(root.getTree(PathUtils.ROOT_PATH));
        ContentSession cs = ir.getContentSession();
        assertSame(AuthInfo.EMPTY, cs.getAuthInfo());
        assertNull(cs.getWorkspaceName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromInvalidRoot() {
        new ImmutableRoot(mock(Root.class));
    }
}
