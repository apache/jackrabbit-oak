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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
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

public class SessionImplCapabilityWithMountInfoProviderTest {
    
    private Session adminSession;

    @Before
    public void prepare() throws Exception {
        MountInfoProvider mip = Mounts.newBuilder().readOnlyMount("ro", "/private").build();
        
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
    }

    @Test
    public void addNode() throws Exception {
        
        // unable to add nodes in the read-only mount
        assertFalse("Must not be able to add a child not under the private mount root",
                adminSession.hasCapability("addNode", adminSession.getNode("/private"), new String[] {"foo"}));
        assertFalse("Must not be able to add a child not under the private mount", 
                adminSession.hasCapability("addNode", adminSession.getNode("/private/foo"), new String[] {"bar"}));
        // able to add nodes outside the read-only mount
        assertTrue("Must be able to add a child node under the root",
                adminSession.hasCapability("addNode", adminSession.getNode("/"), new String[] {"not-private"}));
        // unable to add node at the root of the read-only mount ( even though it already exists )
        assertFalse("Must not be able to add a child node in place of the private mount",
                adminSession.hasCapability("addNode", adminSession.getNode("/"), new String[] {"private"}));
    }
    
    @Test
    public void orderBefore() throws Exception {
        // able to order the root of the mount since the operation is performed on the parent
        assertTrue(adminSession.hasCapability("orderBefore", adminSession.getNode("/private"), null));
        assertFalse(adminSession.hasCapability("orderBefore", adminSession.getNode("/private/foo"), null));
    }
    
    @Test
    public void simpleNodeOperations() throws Exception {
        for ( String operation : new String[] { "setPrimaryType", "addMixin", "removeMixin" , "setProperty", "remove"} )  {
            for ( String privateMountNode : new String[] { "/private", "/private/foo" } ) {
                assertFalse("Unexpected return value for hasCapability(" + operation+ ") on node '" + privateMountNode +"' from the private mount",
                        adminSession.hasCapability(operation, adminSession.getNode(privateMountNode), null));
            }
            String globalMountNode = "/foo";
            assertTrue("Unexpected return value for hasCapability(" + operation+ ") on node '" + globalMountNode +"' from the global mount",
                    adminSession.hasCapability(operation, adminSession.getNode(globalMountNode), null));
        }
    }    

    @Test
    public void itemOperations() throws Exception {
        for ( String operation : new String[] { "setValue", "remove"} )  {
            String privateMountProp = "/private/prop";
            String globalMountProp = "/foo/prop";
            
            assertFalse("Unexpected return value for hasCapability(" + operation+ ") on item '" + privateMountProp +"' from the private mount",
                    adminSession.hasCapability(operation, adminSession.getItem(privateMountProp), null));
            assertTrue("Unexpected return value for hasCapability(" + operation+ ") on item '" + globalMountProp +"' from the global mount",
                    adminSession.hasCapability(operation, adminSession.getItem(globalMountProp), null));
        }        
    }
}
