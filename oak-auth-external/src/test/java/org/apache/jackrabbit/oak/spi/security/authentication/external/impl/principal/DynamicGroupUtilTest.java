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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import javax.jcr.RepositoryException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicGroupUtilTest extends AbstractSecurityTest {
    
    @Test
    public void findGroupIdInHierarchy() throws RepositoryException {
        Group gr = getUserManager(root).createGroup("grId");
        Tree tree = root.getTree(gr.getPath());
        
        assertEquals("grId", DynamicGroupUtil.findGroupIdInHierarchy(tree));

        Tree child = TreeUtil.addChild(tree, "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEquals("grId", DynamicGroupUtil.findGroupIdInHierarchy(child));

        Tree membersList = TreeUtil.addChild(tree, UserConstants.REP_MEMBERS_LIST, UserConstants.NT_REP_MEMBER_REFERENCES_LIST);
        assertEquals("grId", DynamicGroupUtil.findGroupIdInHierarchy(membersList));

        Tree members = TreeUtil.addChild(membersList, "any", UserConstants.NT_REP_MEMBER_REFERENCES);
        assertEquals("grId", DynamicGroupUtil.findGroupIdInHierarchy(members));

        assertNull(DynamicGroupUtil.findGroupIdInHierarchy(tree.getParent()));
        assertNull(DynamicGroupUtil.findGroupIdInHierarchy(root.getTree(PathUtils.ROOT_PATH)));
    }
    
    @Test
    public void testHasStoredMemberInfoFails() throws RepositoryException {
        Group gr = when(mock(Group.class).getPath()).thenThrow(new RepositoryException()).getMock();
        assertFalse(DynamicGroupUtil.hasStoredMemberInfo(gr, root));
    }
}