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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CommitValueResolverTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private CountingDocumentStore store = new CountingDocumentStore(new MemoryDocumentStore());

    private DocumentNodeStore ns;

    private CachingCommitValueResolver resolver;

    @Before
    public void setup() {
        ns = builderProvider.newBuilder().setDocumentStore(store)
                .setUpdateLimit(20).setAsyncDelay(0).getNodeStore();
        resolver = newCachingCommitValueResolver(0);
    }

    @Test
    public void unknownRevision() throws Exception {
        Revision oldRevision = ns.newRevision();
        addNode("/foo");
        Revision newRevision = ns.newRevision();
        NodeDocument foo = getDocument("/foo");
        NodeDocument root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertNull(resolver.resolve(oldRevision, foo));
        assertNull(resolver.resolve(oldRevision, root));
        assertNull(resolver.resolve(newRevision, foo));
        assertNull(resolver.resolve(newRevision, root));

        // trigger sweeper
        ns.runBackgroundOperations();

        // must still not report as committed
        foo = getDocument("/foo");
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertNull(resolver.resolve(oldRevision, foo));
        assertNull(resolver.resolve(oldRevision, root));
        assertNull(resolver.resolve(newRevision, foo));
        assertNull(resolver.resolve(newRevision, root));
    }

    @Test
    public void committedTrunkCommit() throws Exception {
        Revision r = addNode("/foo");
        NodeDocument foo = getDocument("/foo");
        NodeDocument root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));

        // trigger sweeper
        ns.runBackgroundOperations();

        // must still report as committed
        foo = getDocument("/foo");
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));
    }

    @Test
    public void committedTrunkCommitValueMovedToPreviousDoc() throws Exception {
        Revision r = addNode("/foo");
        // add changes until the revision moves to a previous document
        assertTrue(getDocument("/").getLocalRevisions().containsKey(r));
        while (getDocument("/").getLocalRevisions().containsKey(r)) {
            someChange("/");
            ns.runBackgroundUpdateOperations();
        }
        NodeDocument foo = getDocument("/foo");
        NodeDocument root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));

        // trigger sweeper
        ns.runBackgroundOperations();

        // must still report as committed
        foo = getDocument("/foo");
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));
    }

    @Test
    public void committedTrunkCommitMovedToPreviousDoc() throws Exception {
        String path = "/foo";
        Revision r = addNode(path);
        removeNode(path);
        addNode(path);
        // add changes until the revision moves to a previous document
        assertTrue(getDocument("/foo").getLocalCommitRoot().containsKey(r));
        while (getDocument("/foo").getLocalCommitRoot().containsKey(r)) {
            someChange("/foo");
            ns.runBackgroundUpdateOperations();
        }
        NodeDocument foo = getDocument("/foo");
        NodeDocument root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));

        // trigger sweeper
        ns.runBackgroundOperations();

        // must still report as committed
        foo = getDocument("/foo");
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals("c", resolver.resolve(r, foo));
        assertEquals("c", resolver.resolve(r, root));
    }

    @Test
    public void branchCommit() throws Exception {
        String path = "/foo";
        NodeBuilder builder = addNodeBranched(path);
        Revision r = getDocument("/").getLocalRevisions().firstKey();
        String value = getDocument("/").getLocalRevisions()
                .entrySet().iterator().next().getValue();
        NodeDocument foo = getDocument(path);
        NodeDocument root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals(value, resolver.resolve(r, foo));
        assertEquals(value, resolver.resolve(r, root));

        // add another commit and run the sweeper
        addNode("/bar");
        ns.runBackgroundUpdateOperations();

        // must still report the same value
        foo = getDocument(path);
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        assertEquals(value, resolver.resolve(r, foo));
        assertEquals(value, resolver.resolve(r, root));

        // now merge the branch
        TestUtils.merge(ns, builder);

        // now must report the committed revision
        foo = getDocument(path);
        root = getDocument("/");
        assertNotNull(foo);
        assertNotNull(root);
        value = foo.resolveCommitValue(r);
        assertTrue(Utils.isCommitted(value));
        assertEquals(value, root.resolveCommitValue(r));
        assertEquals(value, resolver.resolve(r, foo));
        assertEquals(value, resolver.resolve(r, root));
    }

    @Test
    public void cacheEmptyCommitValue() throws Exception {
        addNode("/foo");
        // add changes and remove commit value
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").setProperty("p", "v");
        builder.child("bar");
        TestUtils.merge(ns, builder);
        Revision commitRev = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(commitRev);
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
        NodeDocument.removeRevision(op, commitRev);
        assertNotNull(store.findAndUpdate(NODES, op));

        waitABit();

        CommitValueResolver cvr = newCachingCommitValueResolver(100)
                .withEmptyCommitValueCache(true, ns.getClock(), 0);
        NodeDocument foo = getDocument("/foo");

        // resolver without negative cache will look up previous docs every time
        assertNull(resolver.resolve(commitRev, foo));
        assertThat(countDocumentLookups(() -> resolver.resolve(commitRev, foo)), greaterThan(0));

        // resolver with negative cache will look up only the first time
        assertNull(cvr.resolve(commitRev, foo));
        assertThat(countDocumentLookups(() -> cvr.resolve(commitRev, foo)), equalTo(0));
    }

    private int countDocumentLookups(Callable<?> c) throws Exception {
        int numCalls = store.getNumFindCalls(NODES);
        c.call();
        return store.getNumFindCalls(NODES) - numCalls;
    }

    private void waitABit() throws InterruptedException {
        ns.getClock().getTimeIncreasing();
        ns.getClock().getTimeIncreasing();
    }

    private Revision addNode(String path) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder nb = builder;
        for (String name : PathUtils.elements(path)) {
            nb = nb.child(name);
        }
        TestUtils.merge(ns, builder);
        return ns.getHeadRevision().getRevision(ns.getClusterId());
    }

    private NodeBuilder addNodeBranched(String path) {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder nb = builder;
        for (String name : PathUtils.elements(path)) {
            nb = nb.child(name);
        }
        int numRevEntries = getNumRevisions("/");
        int i = 0;
        while (numRevEntries == getNumRevisions("/")) {
            nb.setProperty("p-" + i++, "v");
        }
        return builder;
    }

    private Revision removeNode(String path) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder nb = builder;
        for (String name : PathUtils.elements(path)) {
            nb = nb.child(name);
        }
        nb.remove();
        TestUtils.merge(ns, builder);
        return ns.getHeadRevision().getRevision(ns.getClusterId());
    }

    private NodeDocument getDocument(String path) {
        return ns.getDocumentStore().find(NODES, getIdFromPath(path));
    }

    private void someChange(String path) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder nb = builder;
        for (String name : PathUtils.elements(path)) {
            nb = nb.child(name);
        }
        long value = 0;
        if (nb.hasProperty("p")) {
            value = nb.getProperty("p").getValue(Type.LONG) + 1;
        }
        nb.setProperty("p", value);
        TestUtils.merge(ns, builder);
    }

    private int getNumRevisions(String path) {
        NodeDocument doc = getDocument(path);
        return doc != null ? doc.getLocalRevisions().size() : 0;
    }

    private CachingCommitValueResolver newCachingCommitValueResolver(int cacheSize) {
        return new CachingCommitValueResolver(cacheSize, () -> ns.getSweepRevisions());
    }
}
