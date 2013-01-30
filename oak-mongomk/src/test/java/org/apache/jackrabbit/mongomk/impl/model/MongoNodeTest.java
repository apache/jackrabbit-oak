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
package org.apache.jackrabbit.mongomk.impl.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class MongoNodeTest {

    @Test
    public void copyOriginalNotChanged() throws Exception {

        // Create a node with properties
        MongoNode node = new MongoNode();
        node.setBranchId("branchId");
        List<String> children = new ArrayList<String>();
        children.add("child");
        node.setChildren(children);
        node.setDeleted(true);
        node.setPath("path");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("key", "value");
        node.setProperties(properties);
        node.setRevisionId(0);

        // Copy the node and change properties
        MongoNode copy = node.copy();
        copy.setBranchId("branchId2");
        List<String> copyChildren = copy.getChildren();
        copyChildren.add("child2");
        copy.setDeleted(false);
        copy.setPath("path2");
        Map<String, Object> copyProperties = copy.getProperties();
        copyProperties.put("key", "valuee");
        copyProperties.put("key2", "value2");
        copy.setRevisionId(1);

        // Assert that original node did not change
        assertEquals("branchId", node.getBranchId());
        children = node.getChildren();
        assertEquals(1, children.size());
        assertEquals("child", children.get(0));
        assertTrue(node.isDeleted());
        assertEquals("path", node.getPath());
        properties = node.getProperties();
        assertEquals(1, properties.size());
        assertEquals("value", properties.get("key"));
        assertTrue(0 == node.getRevisionId());
    }
}
