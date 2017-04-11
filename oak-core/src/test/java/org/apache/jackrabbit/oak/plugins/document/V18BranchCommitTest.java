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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class V18BranchCommitTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    @Before
    public void setup() {
        ns = builderProvider.newBuilder().setUpdateLimit(10)
                .setAsyncDelay(0).getNodeStore();
    }

    @Test
    public void addNode() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        triggerBranchCommit(builder);
        TestUtils.merge(ns, builder);

        NodeDocument doc = docForPath("/foo");
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
    }

    @Test
    public void removeNode() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        TestUtils.merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").remove();
        triggerBranchCommit(builder);
        TestUtils.merge(ns, builder);
        NodeDocument doc = docForPath("/foo");
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
    }

    @Test
    public void addProperty() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        TestUtils.merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").setProperty("p", "v");
        triggerBranchCommit(builder);
        TestUtils.merge(ns, builder);
        NodeDocument doc = docForPath("/foo");
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
    }

    @Test
    public void updateProperty() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").setProperty("p", "v");
        TestUtils.merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").setProperty("p", "z");
        triggerBranchCommit(builder);
        TestUtils.merge(ns, builder);
        NodeDocument doc = docForPath("/foo");
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
    }

    @Test
    public void removeProperty() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").setProperty("p", "v");
        TestUtils.merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").removeProperty("p");
        triggerBranchCommit(builder);
        TestUtils.merge(ns, builder);
        NodeDocument doc = docForPath("/foo");
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
    }

    private void triggerBranchCommit(NodeBuilder builder) {
        NodeBuilder payload = builder.child(UUID.randomUUID().toString());
        int numRevisionEntries = getRootRevisionCount();
        int i = 0;
        do {
            payload.child("node-" + i++);
        } while (numRevisionEntries == getRootRevisionCount());
    }

    private int getRootRevisionCount() {
        return Utils.getRootDocument(ns.getDocumentStore()).getLocalRevisions().size();
    }

    private NodeDocument docForPath(String path) {
        return ns.getDocumentStore().find(NODES, Utils.getIdFromPath(path));
    }
}
