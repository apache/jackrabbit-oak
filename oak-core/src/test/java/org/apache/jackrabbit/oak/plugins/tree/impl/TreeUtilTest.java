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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TreeUtilTest extends AbstractSecurityTest {

    @Test
    public void testJcrCreatedBy() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", JcrConstants.NT_FOLDER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "userId");
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_CREATEDBY);
        assertNotNull(ps);
        assertEquals("userId", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrCreatedByNullUserId() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", JcrConstants.NT_FOLDER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_CREATEDBY);
        assertNotNull(ps);
        assertEquals("", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrLastModifiedBy() throws Exception {
        Tree ntRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED, ntRoot, "userId");
        TreeUtil.addMixin(tree, NodeTypeConstants.MIX_LASTMODIFIED, ntRoot, "userId");
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        assertNotNull(ps);
        assertEquals("userId", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrLastModifiedByNullUserId() throws Exception {
        Tree ntRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED, ntRoot, null);
        TreeUtil.addMixin(tree, NodeTypeConstants.MIX_LASTMODIFIED, ntRoot, null);
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        assertNotNull(ps);
        assertEquals("", ps.getValue(Type.STRING));
    }
}