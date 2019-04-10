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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitBuilderTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    @Before
    public void before() {
        ns = builderProvider.newBuilder().build();
    }

    @Test
    public void empty() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        assertNull(builder.getBaseRevision());
        assertEquals(CommitBuilder.PSEUDO_COMMIT_REVISION, builder.getRevision());
        try {
            builder.build();
            fail("CommitBuilder.build() must fail when the builder " +
                    "was created without a commit revision");
        } catch (IllegalStateException e) {
            // expected
        }
        Revision r = ns.newRevision();
        Commit c = builder.build(r);
        assertNotNull(c);
        assertEquals(r, c.getRevision());
        assertNull(c.getBaseRevision());
        assertFalse(c.getModifiedPaths().iterator().hasNext());
        assertTrue(c.isEmpty());
    }

    @Test
    public void emptyWithBaseRevision() {
        RevisionVector baseRev = ns.getHeadRevision();
        CommitBuilder builder = new CommitBuilder(ns, baseRev);
        assertEquals(baseRev, builder.getBaseRevision());
        Commit c = builder.build(ns.newRevision());
        assertNotNull(c);
        assertEquals(baseRev, c.getBaseRevision());
    }

    @Test
    public void buildWithNullRevision() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.build(null);
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void addNode() {
        RevisionVector baseRev = ns.getHeadRevision();
        Path foo = Path.fromString("/foo");
        CommitBuilder builder = new CommitBuilder(ns, baseRev);
        builder.addNode(foo);
        Commit c = builder.build(ns.newRevision());
        assertNotNull(c);
        assertFalse(c.isEmpty());
    }

    @Test
    public void addNodeTwice() {
        RevisionVector baseRev = ns.getHeadRevision();
        Path foo = Path.fromString("/foo");
        CommitBuilder builder = new CommitBuilder(ns, baseRev);
        builder.addNode(foo);
        try {
            builder.addNode(foo);
            fail("Must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("already added"));
        }
    }

    @Test
    public void addNodePathNull() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.addNode((Path) null);
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void addNodeStateNull() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.addNode((DocumentNodeState) null);
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void addNodeState() {
        Path path = Path.fromString("/foo");
        DocumentNodeState foo = addNode("foo");

        CommitBuilder builder = new CommitBuilder(ns, null);
        builder.addNode(foo);
        Commit c = builder.build(ns.newRevision());
        UpdateOp up = c.getUpdateOperationForNode(path);
        UpdateOp.Operation op = up.getChanges().get(
                new UpdateOp.Key("_deleted", c.getRevision()));
        assertNotNull(op);
    }

    @Test
    public void branchCommit() {
        RevisionVector baseRev = ns.getHeadRevision()
                .update(ns.newRevision().asBranchRevision());
        CommitBuilder builder = new CommitBuilder(ns, baseRev);
        Path path = Path.fromString("/foo");
        builder.addNode(path);
        Revision commitRev = ns.newRevision();
        Commit c = builder.build(commitRev);
        UpdateOp up = c.getUpdateOperationForNode(path);
        UpdateOp.Operation op = up.getChanges().get(
                new UpdateOp.Key("_bc", commitRev));
        assertNotNull(op);
    }

    @Test
    public void removeNode() {
        DocumentNodeState bar = addNode("bar");

        CommitBuilder builder = new CommitBuilder(ns, null);
        Path path = Path.fromString("/bar");

        builder.removeNode(path, bar);
        Commit c = builder.build(ns.newRevision());
        UpdateOp up = c.getUpdateOperationForNode(path);
        UpdateOp.Operation op = up.getChanges().get(
                new UpdateOp.Key("_deleted", c.getRevision()));
        assertNotNull(op);
    }

    @Test
    public void removeNodeTwice() {
        DocumentNodeState bar = addNode("bar");

        CommitBuilder builder = new CommitBuilder(ns, null);
        Path path = Path.fromString("/bar");

        builder.removeNode(path, bar);
        try {
            builder.removeNode(path, bar);
            fail("Must throw DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    @Test
    public void removeNodePathNull() {
        DocumentNodeState bar = addNode("bar");

        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.removeNode(null, bar);
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void removeNodeStateNull() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.removeNode(Path.fromString("/bar"), null);
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void updateProperty() {
        Path path = Path.fromString("/foo");
        CommitBuilder builder = new CommitBuilder(ns, null);
        builder.updateProperty(path, "p", "v");
        Commit c = builder.build(ns.newRevision());
        UpdateOp up = c.getUpdateOperationForNode(path);
        UpdateOp.Operation op = up.getChanges().get(
                new UpdateOp.Key("p", c.getRevision()));
        assertNotNull(op);
    }

    @Test
    public void updatePropertyValueNull() {
        Path path = Path.fromString("/foo");
        CommitBuilder builder = new CommitBuilder(ns, null);
        builder.updateProperty(path, "p", null);
        Commit c = builder.build(ns.newRevision());
        UpdateOp up = c.getUpdateOperationForNode(path);
        UpdateOp.Operation op = up.getChanges().get(
                new UpdateOp.Key("p", c.getRevision()));
        assertNotNull(op);
    }

    @Test
    public void updatePropertyPathNull() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.updateProperty((Path) null, "p", "v");
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void updatePropertyPropertyNull() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        try {
            builder.updateProperty(Path.fromString("/foo"), null, "v");
            expectNPE();
        } catch (NullPointerException e) {
            // expected
        }
    }

    private static void expectNPE() {
        fail("NullPointerException expected");
    }

    private DocumentNodeState addNode(String name) {
        NodeBuilder nb = ns.getRoot().builder();
        assertTrue(nb.child(name).isNew());
        try {
            merge(ns, nb);
        } catch (CommitFailedException e) {
            fail(e.getMessage());
        }
        NodeState child = ns.getRoot().getChildNode(name);
        assertTrue(child.exists());
        assertTrue(child instanceof DocumentNodeState);
        return (DocumentNodeState) child;
    }
}
