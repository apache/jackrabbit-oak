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
package org.apache.jackrabbit.oak.plugins.index.old.mk.mem;

import junit.framework.Assert;

import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeId;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeMap;
import org.junit.Test;

/**
 * Test in-memory node objects.
 */
public class MemoryNodeTest {

    @Test
    public void addChildNodes() {
        NodeMap map = new NodeMap();

        map.setMaxMemoryChildren(2);
        map.setDescendantInlineCount(-1);

        NodeImpl n = new NodeImpl(map, 0);
        Assert.assertEquals("{}", n.asString());
        n.setId(NodeId.get(255));
        Assert.assertEquals("nff={};", n.asString());
        n.setPath("/test");
        Assert.assertEquals("nff={};/* /test */", n.toString());
        n = n.createClone(10);
        Assert.assertEquals("{}", n.asString());
        NodeImpl a = new NodeImpl(map, 0);
        map.addNode(a);
        NodeImpl b = new NodeImpl(map, 0);
        map.addNode(b);
        NodeImpl c = new NodeImpl(map, 0);
        map.addNode(c);
        NodeImpl d = new NodeImpl(map, 0);
        map.addNode(d);
        n = n.cloneAndAddChildNode("a", false, null, a, 11);
        n = n.cloneAndSetProperty("x", "1", 12);
        n.setId(NodeId.get(3));
        Assert.assertEquals("n3={\"x\":1,\"a\":n1};", n.asString());
        NodeImpl n2 = NodeImpl.fromString(map, n.asString());
        Assert.assertEquals("n3={\"x\":1,\"a\":n1};", n2.asString());

        n = new NodeImpl(map, 0);
        n = n.cloneAndAddChildNode("a", false, null, a, 1);
        Assert.assertEquals("{\"a\":n1}", n.asString());
        n = n.cloneAndAddChildNode("b", false, null, b, 2);
        Assert.assertEquals("{\"a\":n1,\"b\":n2}", n.asString());
        n = n.cloneAndAddChildNode("c", false, null, c, 3);
        Assert.assertEquals("{\"a\":n1,\"b\":n2,\"c\":n3}", n.asString());
        n = n.cloneAndAddChildNode("d", false, null, d, 4);
        Assert.assertEquals("{\":children\":n5,\":names\":\"a\",\":children\":n6,\":names\":\"b\",\":children\":n7,\":names\":\"c\",\":children\":n8,\":names\":\"d\",\n\":childCount\":4}",
                n.asString());
        n2 = NodeImpl.fromString(map, n.asString());
        Assert.assertEquals("{\":children\":n5,\":names\":\"a\",\":children\":n6,\":names\":\"b\",\":children\":n7,\":names\":\"c\",\":children\":n8,\":names\":\"d\",\n\":childCount\":4}",
                n2.asString());
        Assert.assertTrue(n2.exists("a"));
        Assert.assertTrue(n2.exists("b"));
        Assert.assertTrue(n2.exists("c"));
        Assert.assertTrue(n2.exists("d"));

    }

    @Test
    public void inlineChildNodes() {
        NodeMap map = new NodeMap();
        map.setDescendantCount(true);
        map.setDescendantInlineCount(3);

        NodeImpl n = new NodeImpl(map, 0);
        Assert.assertEquals("{}", n.asString());
        NodeImpl a = new NodeImpl(map, 0);
        a.setProperty("name", "\"a\"");
        map.addNode(a);
        NodeImpl b = new NodeImpl(map, 0);
        b.setProperty("name", "\"b\"");
        map.addNode(b);
        NodeImpl c = new NodeImpl(map, 0);
        c.setProperty("name", "\"c\"");
        map.addNode(c);
        NodeImpl d = new NodeImpl(map, 0);
        d.setProperty("name", "\"d\"");
        map.addNode(d);
        n = n.cloneAndAddChildNode("a", false, null, a, 11);
        n = n.cloneAndSetProperty("x", "1", 12);
        n.setId(NodeId.get(3));
        Assert.assertEquals("n3={\"x\":1,\"a\":{\"name\":\"a\"}};", n.asString());
        NodeImpl n2 = NodeImpl.fromString(map, n.asString());
        Assert.assertEquals("n3={\"x\":1,\"a\":{\"name\":\"a\"}};", n2.asString());

        n = new NodeImpl(map, 0);
        n = n.cloneAndAddChildNode("a", false, null, a, 1);
        Assert.assertEquals("{\"a\":{\"name\":\"a\"}}", n.asString());
        n = n.cloneAndAddChildNode("b", false, null, b, 2);
        String ab = "{\"a\":{\"name\":\"a\"},\"b\":{\"name\":\"b\"}}";
        Assert.assertEquals(ab, n.asString());
        n = n.cloneAndAddChildNode("c", false, null, c, 3);
        String abc = "{\"a\":{\"name\":\"a\"},\"b\":{\"name\":\"b\"},\"c\":{\"name\":\"c\"}}";
        Assert.assertEquals(abc, n.asString());
        Assert.assertEquals(3, n.getDescendantCount());
        Assert.assertEquals(3, n.getDescendantInlineCount());
        n2 = NodeImpl.fromString(map, n.asString());
        Assert.assertTrue(n2.exists("a"));
        Assert.assertTrue(n2.exists("b"));
        Assert.assertTrue(n2.exists("c"));
        Assert.assertEquals(3, n2.getDescendantCount());
        Assert.assertEquals(3, n2.getDescendantInlineCount());

        NodeImpl root = new NodeImpl(map, 0);
        root = root.cloneAndAddChildNode("test", false, null, n2, 1);
        Assert.assertEquals("{\":size\":4,\"test\":n1}", root.asString());
        Assert.assertEquals(0, root.getDescendantInlineCount());

        n2 = n2.cloneAndRemoveChildNode("c", 4);
        root = new NodeImpl(map, 0);
        root = root.cloneAndAddChildNode("test", false, null, n2, 1);
        Assert.assertEquals("{\":size\":3,\"test\":" + ab + "}", root.asString());

    }

}
