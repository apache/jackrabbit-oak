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
package org.apache.jackrabbit.oak.jcr;

import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

public class SameNamePropertyNodeTest extends AbstractJCRTest {

    private String sameName = "sameName";
    private Node n;
    private Property p;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        if (!getHelper().getRepository().getDescriptorValue(Repository.OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED).getBoolean()) {
            throw new NotExecutableException("node and property with same name is not supported");
        }

        n = testRootNode.addNode(sameName);
        p = testRootNode.setProperty(sameName, "value");
        superuser.save();
    }

    @Test
    public void testIsSame() throws Exception {
        assertFalse(n.isSame(p));
        assertFalse(p.isSame(n));
    }

    @Test
    public void testNodeExists() throws Exception {
        assertTrue(superuser.nodeExists(n.getPath()));
    }

    @Test
    public void testSessionGetNode() throws Exception {
        Node nn = superuser.getNode(n.getPath());
        assertTrue(n.isSame(nn));
    }

    @Test
    public void testHasNode() throws Exception {
        assertTrue(testRootNode.hasNode(sameName));
    }

    @Test
    public void testGetNode() throws Exception {
        assertTrue(n.isSame(testRootNode.getNode(sameName)));
        assertFalse(n.isSame(p));
    }

    @Test
    public void testPropertyExists() throws Exception {
        assertTrue(superuser.propertyExists(p.getPath()));
    }

    @Test
    public void testSessionGetProperty() throws Exception {
        Property pp = superuser.getProperty(p.getPath());
        assertTrue(p.isSame(pp));
    }

    @Test
    public void testHasProperty() throws Exception {
        assertTrue(testRootNode.hasProperty(sameName));
    }

    @Test
    public void testGetProperty() throws Exception {
        assertTrue(p.isSame(testRootNode.getProperty(sameName)));
        assertFalse(p.isSame(n));
    }

    @Test
    public void testItemExists() throws Exception {
        assertTrue(superuser.itemExists(n.getPath()));
    }

    @Test
    public void testGetItem() throws Exception {
        Item item = superuser.getItem(n.getPath());
        if (item.isNode()) {
            assertTrue(n.isSame(item));
        } else {
            assertTrue(p.isSame(item));
        }
    }

    /**
     * Tests if a nodestore fixture sets the SNNP repository descriptor to true.
     */
    @Test
    public void testNodeStoreSupport() throws Exception {
        NodeStore nodeStore = NodeStoreFixtures.SEGMENT_TAR.createNodeStore();
        JackrabbitRepository repository  = (JackrabbitRepository) new Jcr(nodeStore).createRepository();
        try {
            assertTrue(repository.getDescriptorValue(Repository.OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED).getBoolean());
        } finally {
            repository.shutdown();
        }

    }
}