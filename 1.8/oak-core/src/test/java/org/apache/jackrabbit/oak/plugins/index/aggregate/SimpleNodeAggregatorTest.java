/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator.INCLUDE_ALL;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SimpleNodeAggregatorTest {

    @Test
    public void testNodeName() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(JCR_CONTENT));

        String path = "/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertEquals(newArrayList("/file"), actual);

    }

    @Test
    public void testNodeNameWrongParentType() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE + "_");
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(JCR_CONTENT));

        String path = "/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertTrue(actual.isEmpty());

    }

    @Test
    public void testStarName() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(INCLUDE_ALL));

        String path = "/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertEquals(newArrayList("/file"), actual);

    }

    @Test
    public void testStarNameMoreLevels() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList("*", "*/*", "*/*/*", "*/*/*/*"));

        String path = "/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertEquals(newArrayList("/file"), actual);

    }

    @Test
    public void testStarNameWrongParentType() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE + "_");
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(INCLUDE_ALL));

        String path = "/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertTrue(actual.isEmpty());

    }

    @Test
    public void testCascadingStarName() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder folder = builder.child("folder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER);

        NodeBuilder file = folder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FOLDER, newArrayList("file")).newRuleWithName(NT_FILE,
                newArrayList(INCLUDE_ALL));

        String path = "/folder/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertEquals(newArrayList("/folder/file", "/folder"), actual);

    }

    @Test
    public void testCascadingNodeName() {

        NodeState root = new MemoryNodeStore().getRoot();
        NodeBuilder builder = root.builder();

        NodeBuilder folder = builder.child("folder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER);

        NodeBuilder file = folder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT);

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FOLDER, newArrayList("file")).newRuleWithName(NT_FILE,
                newArrayList(JCR_CONTENT));

        String path = "/folder/file/jcr:content";
        List<String> actual = newArrayList(agg.getParents(
                builder.getNodeState(), path));

        assertEquals(newArrayList("/folder/file", "/folder"), actual);

    }

}
