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

package org.apache.jackrabbit.oak.plugins.index.reference;

import java.util.Collections;
import java.util.List;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newReference;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class ReferenceIndexTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void basicReferenceHandling() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        builder.child("b").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        builder.child("c").setProperty(createProperty("foo", "u2", Type.WEAKREFERENCE));

        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new ReferenceEditorProvider()));

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictProperty("*", Operator.EQUAL, newReference("u1"), PropertyType.REFERENCE);

        assertFilter(f, new ReferenceIndex(), indexed, of("/a", "/b"));

        FilterImpl f2 = createFilter(indexed, NT_BASE);
        f2.restrictProperty("*", Operator.EQUAL, newReference("u2"), PropertyType.WEAKREFERENCE);
        assertFilter(f2, new ReferenceIndex(), indexed, of("/c"));
    }

    @Test
    public void referenceHandlingWithMounts() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").child("x").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        builder.child("b").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        builder.child("c").setProperty(createProperty("foo", "u1", Type.WEAKREFERENCE));

        builder.child("d").setProperty(createProperty("foo", "u2", Type.WEAKREFERENCE));
        builder.child("a").child("y").setProperty(createProperty("foo", "u1", Type.WEAKREFERENCE));

        NodeState after = builder.getNodeState();

        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a")
                .build();

        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new ReferenceEditorProvider().with(mip)));

        ReferenceIndex referenceIndex = new ReferenceIndex(mip);

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictProperty("*", Operator.EQUAL, newReference("u1"), PropertyType.REFERENCE);

        // System.out.println(NodeStateUtils.toString(NodeStateUtils.getNode(indexed, "/oak:index/reference")));
        assertFilter(f, referenceIndex, indexed, of("/a/x", "/b"));

        FilterImpl f2 = createFilter(indexed, NT_BASE);
        f2.restrictProperty("*", Operator.EQUAL, newReference("u1"), PropertyType.WEAKREFERENCE);
        assertFilter(f2, referenceIndex, indexed, of("/c", "/a/y"));
    }

    @Test
    public void removeReferencedNode() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").setProperty(createProperty(JCR_UUID, "u1", Type.STRING));
        builder.child("b").setProperty(createProperty("foo", "u1", Type.REFERENCE));

        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new ReferenceEditorProvider()));

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        builder = indexed.builder();
        builder.getChildNode("a").remove();

        thrown.expect(CommitFailedException.class);
        thrown.expectMessage("OakIntegrity0001: Unable to delete referenced node");
        hook.processCommit(indexed, builder.getNodeState(), CommitInfo.EMPTY);
    }

    @Test
    public void removeWeaklyReferencedNode() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").setProperty(createProperty(JCR_UUID, "u1", Type.STRING));
        builder.child("b").setProperty(createProperty("foo", "u1", Type.WEAKREFERENCE));

        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new ReferenceEditorProvider()));

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        builder = indexed.builder();
        builder.getChildNode("a").remove();

        hook.processCommit(indexed, builder.getNodeState(), CommitInfo.EMPTY);
    }

    @SuppressWarnings("Duplicates")
    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static List<String> assertFilter(Filter filter, QueryIndex queryIndex,
                                             NodeState indexed, List<String> expected) {
        Cursor cursor = queryIndex.query(filter, indexed);
        List<String> paths = newArrayList();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        Collections.sort(paths);
        for (String p : expected) {
            assertTrue("Expected path " + p + " not found", paths.contains(p));
        }
        assertEquals("Result set size is different \nExpected: " +
                expected + "\nActual: " + paths, expected.size(), paths.size());
        return paths;
    }

}