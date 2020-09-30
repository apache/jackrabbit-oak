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
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultRowToAuthorizableTest extends AbstractSecurityTest {

    private ResultRowToAuthorizable groupRrta;
    private ResultRowToAuthorizable userRrta;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        groupRrta = new ResultRowToAuthorizable(new UserManagerImpl(root, getPartialValueFactory(), getSecurityProvider()), root, AuthorizableType.GROUP);
        userRrta = new ResultRowToAuthorizable(new UserManagerImpl(root, getPartialValueFactory(), getSecurityProvider()), root, AuthorizableType.USER);
    }

    private static ResultRow createResultRow(@NotNull String path) {
        PropertyValue propValue = PropertyValues.newPath(path);
        return when(mock(ResultRow.class).getValue(QueryConstants.JCR_PATH)).thenReturn(propValue).getMock();
    }

    @Test
    public void testApplyNullRow() {
        assertNull(groupRrta.apply(null));
    }

    @Test
    public void testRowToNonExistingTree() {
        PropertyValue propValue = PropertyValues.newPath("/path/to/nonExisting/tree");
        ResultRow row = when(mock(ResultRow.class).getValue(QueryConstants.JCR_PATH)).thenReturn(propValue).getMock();
        assertNull(groupRrta.apply(row));
    }

    @Test
    public void testRowToRootTree() {
        assertNull(groupRrta.apply(createResultRow(PathUtils.ROOT_PATH)));
    }

    @Test
    public void testRowToUserTree() throws Exception {
        User user = getTestUser();
        ResultRow row = createResultRow(user.getPath());

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
        ResultRow row = createResultRow(t.getPath());

        assertNull(groupRrta.apply(row));

        Authorizable a = userRrta.apply(row);
        assertNotNull(a);
        assertEquals(user.getID(), a.getID());
    }

    @Test
    public void testRowToNonExistingUserSubTree() throws Exception {
        User user = getTestUser();
        ResultRow row = createResultRow(PathUtils.concat(user.getPath(), "child"));

        assertNull(userRrta.apply(row));
    }

    @Test
    public void testRowNonAccessibleUserTree() throws Exception {
        User user = getTestUser();
        String userPath = user.getPath();

        try (ContentSession cs = login(new SimpleCredentials(user.getID(), user.getID().toCharArray()))) {
            Root r = cs.getLatestRoot();
            ResultRowToAuthorizable rrta = new ResultRowToAuthorizable(new UserManagerImpl(r, getPartialValueFactory(), getSecurityProvider()), r, null);
            assertNull(rrta.apply(createResultRow(userPath)));
        }
    }
}