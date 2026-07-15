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
package org.apache.jackrabbit.oak.plugins.index.diffindex;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class DiffCollectorTest {

    @Test
    public void testUUID() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("a").setProperty("jcr:uuid", "abc");
        builder.child("b").setProperty("jcr:uuid", "xyz");

        NodeState after = builder.getNodeState();

        UUIDDiffCollector collector = new UUIDDiffCollector(root, after);

        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictProperty("jcr:uuid", Operator.EQUAL,
                PropertyValues.newString("abc"));

        Set<String> result = collector.getResults(f);
        Iterator<String> iterator = result.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void testUUIDInner() throws Exception {
        NodeState root = EMPTY_NODE;
        NodeBuilder builder = root.builder();

        builder.child("a").setProperty("jcr:uuid", "abc");
        NodeState before = builder.getNodeState();

        builder = before.builder();
        builder.child("a").child("b").setProperty("jcr:uuid", "xyz");
        NodeState after = builder.getNodeState();

        UUIDDiffCollector collector = new UUIDDiffCollector(before, after);

        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictProperty("jcr:uuid", Operator.EQUAL,
                PropertyValues.newString("xyz"));

        Set<String> result = collector.getResults(f);
        Iterator<String> iterator = result.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("a/b", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testDeepChange() throws Exception {
        NodeState root = EMPTY_NODE;
        NodeBuilder builder = root.builder();

        NodeBuilder b1 = builder.child("rep:security").child(
                "rep:authorizables");
        b1.child("rep:groups").child("t").child("te")
                .child("testGroup_1c22a39f");
        NodeBuilder b2 = b1.child("rep:users");
        b2.child("t").child("te").child("testUser_008e00d9");
        NodeBuilder b3 = b2.child("a");
        b3.child("an").child("anonymous");
        b3.child("ad").child("admin");

        NodeState before = builder.getNodeState();
        builder = before.builder();

        NodeBuilder a1 = builder.child("rep:security")
                .child("rep:authorizables").child("rep:groups").child("t")
                .child("te");
        a1.child("testGroup_1c22a39f").setProperty("jcr:uuid",
                "c6195630-e956-3d4b-8912-479c303bf15a");
        a1.child("testPrincipal_4e6b704e").setProperty("jcr:uuid",
                "ee59b554-76b7-3e27-9fc6-15bda1388894");
        NodeState after = builder.getNodeState();

        UUIDDiffCollector collector = new UUIDDiffCollector(before, after);

        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictProperty("jcr:uuid", Operator.EQUAL, PropertyValues
                .newString("ee59b554-76b7-3e27-9fc6-15bda1388894"));

        Set<String> result = collector.getResults(f);
        Iterator<String> iterator = result.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(
                "rep:security/rep:authorizables/rep:groups/t/te/testPrincipal_4e6b704e",
                iterator.next());
        assertFalse(iterator.hasNext());
    }

}
