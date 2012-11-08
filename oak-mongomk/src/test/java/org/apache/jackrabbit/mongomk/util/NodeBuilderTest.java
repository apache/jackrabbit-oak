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
package org.apache.jackrabbit.mongomk.util;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.util.NodeBuilder;
import org.junit.Test;

public class NodeBuilderTest {

    @Test
    public void testBuildSimpleNodes() throws Exception {
        String json = "{ \"/\" : { \"a\" : { \"b\" : {} , \"c\" : {} } } }";
        Node node = NodeBuilder.build(json);

        NodeImpl node_c = new NodeImpl("/a/c");
        NodeImpl node_b = new NodeImpl("/a/b");
        NodeImpl node_a = new NodeImpl("/a");
        node_a.addChildNodeEntry(node_b);
        node_a.addChildNodeEntry(node_c);
        NodeImpl node_root = new NodeImpl("/");
        node_root.addChildNodeEntry(node_a);

        NodeAssert.assertDeepEquals(node, node_root);
    }

    @Test
    public void testBuildSimpleNodesWithRevisionId() throws Exception {
        String json = "{ \"/#1\" : { \"a#1\" : { \"b#2\" : {} , \"c#2\" : {} } } }";
        Node node = NodeBuilder.build(json);

        NodeImpl node_c = new NodeImpl("/a/c");
        node_c.setRevisionId(2L);

        NodeImpl node_b = new NodeImpl("/a/b");
        node_b.setRevisionId(2L);

        NodeImpl node_a = new NodeImpl("/a");
        node_a.addChildNodeEntry(node_b);
        node_a.addChildNodeEntry(node_c);
        node_a.setRevisionId(1L);

        NodeImpl node_root = new NodeImpl("/");
        node_root.addChildNodeEntry(node_a);
        node_root.setRevisionId(1L);

        NodeAssert.assertDeepEquals(node, node_root);
    }
}
