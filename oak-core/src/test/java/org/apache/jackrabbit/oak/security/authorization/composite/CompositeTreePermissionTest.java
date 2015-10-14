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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CompositeTreePermissionTest extends AbstractSecurityTest {

    private ImmutableTree rootTree;

    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil test = rootNode.addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        rootTree = (ImmutableTree) RootFactory.createReadOnlyRoot(root).getTree("/");
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree("/test").remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private List<AggregatedPermissionProvider> getProviders() {
        return ImmutableList.<AggregatedPermissionProvider>of(new FullScopeProvider(root));
    }

    @Test
    public void testEmptyProviderList() {
        CompositeTreePermission parent = new CompositeTreePermission(ImmutableList.<AggregatedPermissionProvider>of());
        assertFalse(parent.canRead());

        CompositeTreePermission rootTp = new CompositeTreePermission(rootTree, parent);
        assertFalse(rootTp.canRead());

        CompositeTreePermission testTp = new CompositeTreePermission(rootTree.getChild("test"), rootTp);
        assertFalse(testTp.canRead());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetChildOnParent() {
        NodeState childState = rootTree.getChild("test").getNodeState();
        CompositeTreePermission parent = new CompositeTreePermission(getProviders());
        parent.getChildPermission("illegal", childState);
    }

    @Test
    public void testGetChildOnRoot() {
        CompositeTreePermission rootTp = new CompositeTreePermission(rootTree,
                new CompositeTreePermission(getProviders()));
        TreePermission testTp = rootTp.getChildPermission("test", rootTree.getChild("test").getNodeState());
    }

    @Test
    public void testCanRead() throws Exception {
        CompositeTreePermission rootTp = new CompositeTreePermission(rootTree,
                new CompositeTreePermission(getProviders()));

        Field f = CompositeTreePermission.class.getDeclaredField("canRead");
        f.setAccessible(true);

        Object canRead = f.get(rootTp);
        assertNull(canRead);

        rootTp.canRead();

        canRead = f.get(rootTp);
        assertNotNull(canRead);
    }

    @Test
    public void testParentNoRecourse() throws Exception {
        Field f = CompositeTreePermission.class.getDeclaredField("map");
        f.setAccessible(true);

        CompositeTreePermission rootTp = new CompositeTreePermission(rootTree,
                new CompositeTreePermission(ImmutableList.<AggregatedPermissionProvider>of(new NoScopeProvider())));
        assertFalse(rootTp.canRead());
        assertEquals(1, ((Map) f.get(rootTp)).size());


        TreePermission testTp = rootTp.getChildPermission("test", rootTree.getChild("test").getNodeState());
        assertFalse(testTp.canRead());
        assertTrue(((Map) f.get(testTp)).isEmpty());
    }
}