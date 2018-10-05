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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

/**
 * {@code AutoCreatedItemsTest} checks if auto-created nodes and properties
 * are added correctly as defined in the node type definition.
 */
public class AutoCreatedItemsTest extends AbstractRepositoryTest {

    public AutoCreatedItemsTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void autoCreatedItems() throws Exception {
        Session s = getAdminSession();
        new TestContentLoader().loadTestContent(s);
        Node test = s.getRootNode().addNode("test", "test:autoCreate");
        assertTrue(test.hasProperty("test:property"));
        assertEquals("default value", test.getProperty("test:property").getString());
        assertTrue(test.hasProperty("test:propertyMulti"));
        assertArrayEquals(new Value[]{s.getValueFactory().createValue("value1"),
                s.getValueFactory().createValue("value2")},
                test.getProperty("test:propertyMulti").getValues());

        assertTrue(test.hasNode("test:folder"));
        Node folder = test.getNode("test:folder");
        assertEquals("nt:folder", folder.getPrimaryNodeType().getName());
        assertTrue(folder.hasProperty("jcr:created"));
        assertTrue(folder.hasProperty("jcr:createdBy"));
    }
}
