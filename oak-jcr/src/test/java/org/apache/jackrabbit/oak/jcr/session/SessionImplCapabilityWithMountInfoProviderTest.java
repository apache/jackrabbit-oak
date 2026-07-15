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
package org.apache.jackrabbit.oak.jcr.session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.CompositeNodeStore;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import java.util.Collections;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class SessionImplCapabilityWithMountInfoProviderTest {
    
    private static final String MOUNT_PATH = "/private";
    private static final String MOUNT_PATH_FOO = "/private/foo";
    private static final String GLOBAL_PATH_FOO = "/foo";
    
    
    private Session adminSession;
    private Session guestSession;

    @Before
    public void prepare() throws Exception {
        MountInfoProvider mip = Mounts.newBuilder().readOnlyMount("ro", MOUNT_PATH).build();
        
        MemoryNodeStore roStore = new MemoryNodeStore();
        {
            NodeBuilder builder = roStore.getRoot().builder();
            builder
                .child("private").setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED)
                    .setProperty("prop", "value")
                .child("foo").setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED);
            roStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        {
            NodeBuilder builder = globalStore.getRoot().builder();
            builder
                .child("foo").setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED)
                    .setProperty("prop", "value")
                .child("bar").setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED);
            globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        
        CompositeNodeStore store = new CompositeNodeStore.Builder(mip, globalStore)
            .addMount("ro", roStore)
            .build();
        
        Whiteboard whiteboard = new  DefaultWhiteboard();
        whiteboard.register(MountInfoProvider.class, mip, Collections.emptyMap());
        
        Jcr jcr = new Jcr(store).with(whiteboard);
        jcr.createContentRepository();
        Repository repository = jcr.createRepository();
        
        adminSession = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        guestSession = repository.login(new GuestCredentials());
    }

    @Test
    public void addNode() throws Exception {
        
        // unable to add nodes in the read-only mount
        assertFalse("Must not be able to add a child not under the private mount root",
                adminSession.hasCapability("addNode", adminSession.getNode(MOUNT_PATH), new String[] {"foo"}));
        assertFalse("Must not be able to add a child not under the private mount", 
                adminSession.hasCapability("addNode", adminSession.getNode(MOUNT_PATH_FOO), new String[] {"bar"}));
        // able to add nodes outside the read-only mount
        assertTrue("Must be able to add a child node under the root",
                adminSession.hasCapability("addNode", adminSession.getNode(ROOT_PATH), new String[] {"not-private"}));
        // unable to add node at the root of the read-only mount ( even though it already exists )
        assertFalse("Must not be able to add a child node in place of the private mount",
                adminSession.hasCapability("addNode", adminSession.getNode(ROOT_PATH), new String[] {"private"}));
    }

    @Test
    public void addNodeWithRelativePath() throws Exception {
        String relativePath = "rel/path/for/add/node";
        // unable to add nodes in the read-only mount
        assertFalse("Must not be able to add a child not under the private mount root",
                adminSession.hasCapability("addNode", adminSession.getNode(MOUNT_PATH), new String[] {relativePath}));
        assertFalse("Must not be able to add a child not under the private mount",
                adminSession.hasCapability("addNode", adminSession.getNode(MOUNT_PATH_FOO), new String[] {relativePath}));
        // able to add nodes outside the read-only mount
        assertTrue("Must be able to add a child node under the root",
                adminSession.hasCapability("addNode", adminSession.getNode(ROOT_PATH), new String[] {relativePath}));
        // unable to add node at the root of the read-only mount ( even though it already exists )
        assertFalse("Must not be able to add a child node in place of the private mount",
                adminSession.hasCapability("addNode", adminSession.getNode(ROOT_PATH), new String[] {"private/rel/path"}));
    }

    @Test
    public void addNodeWithInvalidPathArgument() throws Exception {
        Node n = adminSession.getNode(ROOT_PATH);
        assertFalse("Must not be able to add a child if invalid path argument is passed",
                adminSession.hasCapability("addNode", n, new String[] {null}));
        assertFalse("Must not be able to add a child if no path argument is passed",
                adminSession.hasCapability("addNode", n, new String[] {}));
        assertFalse("Must not be able to add a child if no path argument is passed",
                adminSession.hasCapability("addNode", n, null));
    }


    @Test
    public void addNodeMissingPermissions() throws Exception {
        // FIXME: guest does not have access to the root node in the test setup
        Node n = adminSession.getNode(ROOT_PATH);
        // unable to add nodes outside the read-only mount
        assertFalse("Must be not able to add a child node under the root",
                guestSession.hasCapability("addNode", n, new String[] {"not-private"}));
        // unable to add node at the root of the read-only mount ( even though it already exists )
        assertFalse("Must not be able to add a child node in place of the private mount",
                guestSession.hasCapability("addNode", n, new String[] {"private"}));
    }
    
    @Test
    public void orderBefore() throws Exception {
        // able to order the root of the mount since the operation is performed on the parent
        assertTrue(adminSession.hasCapability("orderBefore", adminSession.getNode(MOUNT_PATH), null));
        assertFalse(adminSession.hasCapability("orderBefore", adminSession.getNode(MOUNT_PATH_FOO), null));
        // root node can never be reordered
        assertFalse(adminSession.hasCapability("orderBefore", adminSession.getNode(ROOT_PATH), null));
    }
    
    @Test
    public void simpleNodeOperations() throws Exception {
        for ( String operation : new String[] { "setPrimaryType", "addMixin", "removeMixin" , "setProperty", "remove"} )  {
            for ( String privateMountNode : new String[] { MOUNT_PATH, MOUNT_PATH_FOO } ) {
                assertFalse("Unexpected return value for hasCapability(" + operation+ ") on node '" + privateMountNode +"' from the private mount",
                        adminSession.hasCapability(operation, adminSession.getNode(privateMountNode), null));
            }
            assertTrue("Unexpected return value for hasCapability(" + operation+ ") on node '" + GLOBAL_PATH_FOO +"' from the global mount",
                    adminSession.hasCapability(operation, adminSession.getNode(GLOBAL_PATH_FOO), null));
        }
    }

    @Test
    public void simpleNodeOperationsMissingPermission() throws Exception {
        // FIXME: guest does not have access to the root node in the test setup
        Node n = adminSession.getNode(ROOT_PATH);
        for ( String operation : new String[] { "setPrimaryType", "addMixin", "removeMixin" , "setProperty", "remove"} )  {
            assertFalse("Unexpected return value for hasCapability(" + operation+ ") on node '" + ROOT_PATH +"' from the global mount",
                    guestSession.hasCapability(operation, n, null));
        }
    }

    @Test
    public void uncoveredNodeOperation() throws Exception {
        String unknownOperation = "unknown";
        assertFalse("Unexpected return value for hasCapability('+unknownOperation+') on node '/private' from the private mount.", 
                adminSession.hasCapability(unknownOperation, adminSession.getNode(MOUNT_PATH), null));
        assertTrue("Unexpected return value for hasCapability('+unknownOperation+') on node '/foo' from the global mount.",
                adminSession.hasCapability(unknownOperation, adminSession.getNode(GLOBAL_PATH_FOO), null));
    }

    @Test
    public void itemOperations() throws Exception {
        for ( String operation : new String[] { "setValue", "remove", "unknown"} )  {
            String privateMountProp = "/private/prop";
            String globalMountProp = "/foo/prop";
            
            assertFalse("Unexpected return value for hasCapability(" + operation+ ") on item '" + privateMountProp +"' from the private mount",
                    adminSession.hasCapability(operation, adminSession.getItem(privateMountProp), null));
            assertTrue("Unexpected return value for hasCapability(" + operation+ ") on item '" + globalMountProp +"' from the global mount",
                    adminSession.hasCapability(operation, adminSession.getItem(globalMountProp), null));
        }        
    }
    
    @Test
    public void policyWriteOperation() throws Exception {
        AccessControlManager acMgr = adminSession.getAccessControlManager();
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        for (String operation : new String[]{"setPolicy", "removePolicy"}) {
            assertFalse("Unexpected return value for hasCapability(" + operation + ") on the private mount",
                    adminSession.hasCapability(operation, acMgr, new Object[] {MOUNT_PATH_FOO, policy}));
            assertTrue("Unexpected return value for hasCapability(" + operation + ") on the global mount",
                    adminSession.hasCapability(operation, acMgr, new Object[] {GLOBAL_PATH_FOO, policy}));
        }
        verifyNoInteractions(policy);
    }

    @Test
    public void policyWriteOperationNullPath() throws Exception {
        AccessControlManager acMgr = adminSession.getAccessControlManager();
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        for (String operation : new String[]{"setPolicy", "removePolicy"}) {
            assertTrue("Unexpected return value for hasCapability(" + operation +") on the global mount (null path)",
                    adminSession.hasCapability(operation, acMgr, new Object[] {null, policy}));
        }
        verifyNoInteractions(policy);
    }

    @Test
    public void policyWriteOperationMissingPermission() throws Exception {
        AccessControlManager acMgr = guestSession.getAccessControlManager();
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        for (String operation : new String[]{"setPolicy", "removePolicy"}) {
            assertFalse("Unexpected return value for hasCapability(" + operation + ") on the private mount",
                    guestSession.hasCapability(operation, acMgr, new Object[] {MOUNT_PATH_FOO, policy}));
            assertFalse("Unexpected return value for hasCapability(" + operation + ") on the global mount",
                    guestSession.hasCapability(operation, acMgr, new Object[] {GLOBAL_PATH_FOO, policy}));
        }
        verifyNoInteractions(policy);
    }

    @Test
    public void policyWriteOperationMissingArguments() throws Exception {
        AccessControlManager acMgr = guestSession.getAccessControlManager();
        for (String operation : new String[]{"setPolicy", "removePolicy"}) {
            // missing arguments => cannot perform capability check
            assertFalse("Unexpected return value for hasCapability(" + operation + ") with insufficient arguments.",
                    guestSession.hasCapability(operation, acMgr, new Object[0]));
            assertFalse("Unexpected return value for hasCapability(" + operation + ") with null arguments.",
                    guestSession.hasCapability(operation, acMgr, null));
        }
    }

    @Test
    public void uncoveredAccessControlMethod() throws Exception {
        AccessControlManager acMgr = guestSession.getAccessControlManager();
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        for (String operation : new String[]{"getApplicablePolicies", "getPolicies", "getEffectivePolicies"}) {
            assertTrue("Unexpected return value for hasCapability(" + operation + ").",
                    guestSession.hasCapability(operation, acMgr, new Object[] {GLOBAL_PATH_FOO, policy}));
        }
        verifyNoInteractions(policy);
    }
    
    @Test
    public void uncoveredTarget() throws Exception {
        assertTrue("Default return value for unsupported method/target object must be true.", 
                guestSession.hasCapability("unknownMethod", new Object(), null));
    }
}
