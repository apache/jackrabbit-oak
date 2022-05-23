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
package org.apache.jackrabbit.oak.security.user.query;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.user.AbstractUserTest;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultRowToAuthorizableTest extends AbstractUserTest {

    private ResultRowToAuthorizable groupRrta;
    private ResultRowToAuthorizable userRrta;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        groupRrta = createResultRowToAuthorizable(root, AuthorizableType.GROUP);
        userRrta = createResultRowToAuthorizable(root, AuthorizableType.USER);
    }

    @NotNull
    private ResultRowToAuthorizable createResultRowToAuthorizable(@NotNull Root r, @Nullable AuthorizableType targetType) {
        UserManagerImpl umgr = createUserManagerImpl(r);
        return new ResultRowToAuthorizable(umgr, r, targetType, new String[0]);
    }

    private static ResultRow createResultRow(@NotNull Tree tree) {
        return when(mock(ResultRow.class).getTree(null)).thenReturn(tree).getMock();
    }

    @Test
    public void testApplyNullRow() {
        assertNull(groupRrta.apply(null));
    }

    @Test
    public void testRowToNonExistingTree() {
        assertNull(groupRrta.apply(createResultRow(root.getTree("/path/to/nonExisting/tree"))));
    }

    @Test
    public void testRowToRootTree() {
        assertNull(groupRrta.apply(createResultRow(root.getTree(PathUtils.ROOT_PATH))));
    }

    @Test
    public void testRowToUserTree() throws Exception {
        User user = getTestUser();
        ResultRow row = createResultRow(root.getTree(user.getPath()));

        assertNull(groupRrta.apply(row));

        Authorizable a = userRrta.apply(row);
        assertNotNull(a);
        assertEquals(user.getID(), a.getID());
    }

    @Test
    public void testRowToUserSubTree() throws Exception {
        User user = getTestUser();
        Tree t = root.getTree(user.getPath());
        t = TreeUtil.addChild(t, "child", NT_OAK_UNSTRUCTURED);
        ResultRow row = createResultRow(t);

        assertNull(groupRrta.apply(row));

        Authorizable a = userRrta.apply(row);
        assertNotNull(a);
        assertEquals(user.getID(), a.getID());
    }

    @Test
    public void testRowToNonExistingUserSubTree() throws Exception {
        User user = getTestUser();
        Tree tree = root.getTree(user.getPath()).getChild("child");
        ResultRow row = createResultRow(tree);

        assertNull(userRrta.apply(row));
    }

    @Test
    public void testRowNonAccessibleUserTree() throws Exception {
        User user = getTestUser();
        String userPath = user.getPath();

        try (ContentSession cs = login(new SimpleCredentials(user.getID(), user.getID().toCharArray()))) {
            Root r = cs.getLatestRoot();
            ResultRowToAuthorizable rrta = createResultRowToAuthorizable(r, null);
            assertNull(rrta.apply(createResultRow(r.getTree(userPath))));
        }
    }
}