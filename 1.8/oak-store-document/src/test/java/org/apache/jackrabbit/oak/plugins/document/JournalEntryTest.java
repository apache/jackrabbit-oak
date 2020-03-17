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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JournalEntry}.
 */
public class JournalEntryTest {

    @Test
    public void applyTo() throws Exception {
        DiffCache cache = new MemoryDiffCache(new DocumentMK.Builder());
        List<String> paths = Lists.newArrayList();
        addRandomPaths(paths);
        StringSort sort = JournalEntry.newSorter();
        add(sort, paths);
        RevisionVector from = new RevisionVector(new Revision(1, 0, 1));
        RevisionVector to = new RevisionVector(new Revision(2, 0, 1));
        sort.sort();
        JournalEntry.applyTo(sort, cache, "/", from, to);

        for (String p : paths) {
            String changes = cache.getChanges(from, to, p, null);
            assertNotNull("missing changes for " + p, changes);
            for (String c : getChildren(changes)) {
                assertTrue(paths.contains(PathUtils.concat(p, c)));
            }
        }
        sort.close();
    }

    @Test
    public void applyToWithPath() throws Exception {
        DiffCache cache = new MemoryDiffCache(new DocumentMK.Builder());
        StringSort sort = JournalEntry.newSorter();
        sort.add("/");
        sort.add("/foo");
        sort.add("/foo/a");
        sort.add("/foo/b");
        sort.add("/bar");
        sort.add("/bar/a");
        sort.add("/bar/b");
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        sort.sort();
        JournalEntry.applyTo(sort, cache, "/foo", from, to);
        assertNotNull(cache.getChanges(from, to, "/foo", null));
        assertNotNull(cache.getChanges(from, to, "/foo/a", null));
        assertNotNull(cache.getChanges(from, to, "/foo/b", null));
        assertNull(cache.getChanges(from, to, "/bar", null));
        assertNull(cache.getChanges(from, to, "/bar/a", null));
        assertNull(cache.getChanges(from, to, "/bar/b", null));
    }

    //OAK-3494
    @Test
    public void useParentDiff() throws Exception {
        DiffCache cache = new MemoryDiffCache(new DocumentMK.Builder());
        RevisionVector from = new RevisionVector(new Revision(1, 0, 1));
        RevisionVector to = new RevisionVector(new Revision(2, 0, 1));
        RevisionVector unjournalled = new RevisionVector(new Revision(3, 0, 1));

        //Put one entry for (from, to, "/a/b")->["c1", "c2"] manually
        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append("/a/b", "^\"c1\":{}^\"c2\":{}");
        entry.done();

        //NOTE: calling validateCacheUsage fills the cache with an empty diff for the path being validated.
        //So, we need to make sure that each validation is done on a separate path.

        //Cases that cache can answer (only c1 and c2 sub-trees are possibly changed)
        validateCacheUsage(cache, from, to, "/a/b/c3", true);
        validateCacheUsage(cache, from, to, "/a/b/c4/e/f/g", true);

        //Cases that cache can't answer
        validateCacheUsage(cache, from, to, "/a/b/c1", false); //cached entry says that c1 sub-tree is changed
        validateCacheUsage(cache, from, to, "/a/b/c2/d", false); //cached entry says that c2 sub-tree is changed
        validateCacheUsage(cache, from, to, "/c", false);//there is no cache entry for the whole hierarchy

        //Fill cache using journal
        List<String> paths = Lists.newArrayList("/content/changed", "/content/changed1/child1");
        StringSort sort = JournalEntry.newSorter();
        add(sort, paths);
        sort.sort();
        JournalEntry.applyTo(sort, cache, "/", from, to);

        validateCacheUsage(cache, from, to, "/topUnchanged", true);
        validateCacheUsage(cache, from, to, "/content/changed/unchangedLeaf", true);
        validateCacheUsage(cache, from, to, "/content/changed1/child2", true);

        //check against an unjournalled revision (essentially empty cache)
        validateCacheUsage(cache, from, unjournalled, "/unjournalledPath", false);
    }

    @Test
    public void fillExternalChanges() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);
        Set<String> paths = Sets.newHashSet();
        addRandomPaths(paths);
        entry.modified(paths);
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        Revision r3 = new Revision(3, 0, 1);
        UpdateOp op = entry.asUpdateOp(r2);
        assertTrue(store.create(JOURNAL, Collections.singletonList(op)));

        StringSort sort = JournalEntry.newSorter();
        StringSort inv = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, inv, r2, r3, store);
        assertEquals(0, sort.getSize());
        assertEquals(0, inv.getSize());

        JournalEntry.fillExternalChanges(sort, inv, r1, r2, store);
        assertEquals(paths.size(), sort.getSize());
        assertEquals(0, inv.getSize());
        sort.close();

        sort = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, inv, r1, r3, store);
        assertEquals(paths.size(), sort.getSize());
        assertEquals(0, inv.getSize());
        sort.close();
        inv.close();
    }

    @Test
    public void invalidateOnly() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry invalidateEntry = JOURNAL.newDocument(store);
        Set<String> paths = Sets.newHashSet();
        addRandomPaths(paths);
        invalidateEntry.modified(paths);
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        Revision r3 = new Revision(3, 0, 1);
        UpdateOp op = invalidateEntry.asUpdateOp(r1.asBranchRevision());
        assertTrue(store.create(JOURNAL, singletonList(op)));

        JournalEntry entry = JOURNAL.newDocument(store);
        entry.invalidate(singleton(r1));
        op = entry.asUpdateOp(r2);
        assertTrue(store.create(JOURNAL, singletonList(op)));

        StringSort sort = JournalEntry.newSorter();
        StringSort inv = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, inv, r2, r3, store);
        assertEquals(0, sort.getSize());
        assertEquals(0, inv.getSize());

        JournalEntry.fillExternalChanges(sort, inv, r1, r2, store);
        assertEquals(1, sort.getSize());
        assertEquals(paths.size(), inv.getSize());
        inv.close();
        sort.close();

        sort = JournalEntry.newSorter();
        inv = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, inv, r1, r3, store);
        assertEquals(1, sort.getSize());
        assertEquals(paths.size(), inv.getSize());
        sort.close();
        inv.close();
    }

    @Test
    public void fillExternalChanges2() throws Exception {
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        Revision r3 = new Revision(3, 0, 1);
        Revision r4 = new Revision(4, 0, 1);
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);
        entry.modified("/");
        entry.modified("/foo");
        UpdateOp op = entry.asUpdateOp(r2);
        assertTrue(store.create(JOURNAL, Collections.singletonList(op)));

        entry = JOURNAL.newDocument(store);
        entry.modified("/");
        entry.modified("/bar");
        op = entry.asUpdateOp(r4);
        assertTrue(store.create(JOURNAL, Collections.singletonList(op)));

        StringSort sort = externalChanges(r1, r1, store);
        assertEquals(0, sort.getSize());
        sort.close();

        sort = externalChanges(r1, r2, store);
        assertEquals(Sets.newHashSet("/", "/foo"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r1, r3, store);
        assertEquals(Sets.newHashSet("/", "/foo", "/bar"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r1, r4, store);
        assertEquals(Sets.newHashSet("/", "/foo", "/bar"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r2, r2, store);
        assertEquals(0, sort.getSize());
        sort.close();

        sort = externalChanges(r2, r3, store);
        assertEquals(Sets.newHashSet("/", "/bar"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r2, r4, store);
        assertEquals(Sets.newHashSet("/", "/bar"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r3, r3, store);
        assertEquals(0, sort.getSize());
        sort.close();

        sort = externalChanges(r3, r4, store);
        assertEquals(Sets.newHashSet("/", "/bar"), Sets.newHashSet(sort));
        sort.close();

        sort = externalChanges(r4, r4, store);
        assertEquals(0, sort.getSize());
        sort.close();
    }

    @Test
    public void fillExternalChangesWithPath() throws Exception {
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);
        entry.modified("/");
        entry.modified("/foo");
        entry.modified("/foo/a");
        entry.modified("/foo/b");
        entry.modified("/foo/c");
        entry.modified("/bar");
        entry.modified("/bar/a");
        entry.modified("/bar/b");
        entry.modified("/bar/c");

        UpdateOp op = entry.asUpdateOp(r2);
        assertTrue(store.create(JOURNAL, Collections.singletonList(op)));

        StringSort sort = JournalEntry.newSorter();
        StringSort inv = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, inv, "/foo", r1, r2, store, null, null);
        assertEquals(4, sort.getSize());
        assertEquals(0, inv.getSize());
        sort.close();
        inv.close();
    }

    @Test
    public void getRevisionTimestamp() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);
        entry.modified("/foo");
        Revision r = Revision.newRevision(1);
        assertTrue(store.create(JOURNAL,
                Collections.singletonList(entry.asUpdateOp(r))));
        entry = store.find(JOURNAL, JournalEntry.asId(r));
        assertEquals(r.getTimestamp(), entry.getRevisionTimestamp());
    }

    // OAK-4682
    @Test
    public void concurrentModification() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
        try {
            final JournalEntry entry = store.getCurrentJournalEntry();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 100000; i++) {
                        entry.modified("/node-" + i);
                    }
                }
            });
            t.start();
            StringSort sort = JournalEntry.newSorter();
            try {
                entry.addTo(sort, PathUtils.ROOT_PATH);
            } finally {
                sort.close();
            }
            t.join();
        } finally {
            store.dispose();
        }
    }

    @Test
    public void addToWithPath() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);
        entry.modified("/");
        entry.modified("/foo");
        entry.modified("/foo/a");
        entry.modified("/foo/b");
        entry.modified("/foo/c");
        entry.modified("/bar");
        entry.modified("/bar/a");
        entry.modified("/bar/b");
        entry.modified("/bar/c");
        StringSort sort = JournalEntry.newSorter();
        entry.addTo(sort, "/foo");
        assertEquals(4, sort.getSize());
        sort.close();
    }

    @Test
    public void countUpdatedPaths() {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);

        assertEquals("Incorrect number of initial paths", 0, entry.getNumChangedNodes());
        assertFalse("Incorrect hasChanges", entry.hasChanges());

        entry.modified("/foo");
        entry.modified("/bar");
        assertEquals("Incorrect number of paths", 2, entry.getNumChangedNodes());
        assertTrue("Incorrect hasChanges", entry.hasChanges());

        entry.modified(Arrays.asList("/foo1", "/bar1"));
        assertEquals("Incorrect number of paths", 4, entry.getNumChangedNodes());
        assertTrue("Incorrect hasChanges", entry.hasChanges());

        entry.modified("/foo/bar2");
        assertEquals("Incorrect number of paths", 5, entry.getNumChangedNodes());
        assertTrue("Incorrect hasChanges", entry.hasChanges());

        entry.modified("/foo3/bar3");
        assertEquals("Incorrect number of paths", 7, entry.getNumChangedNodes());
        assertTrue("Incorrect hasChanges", entry.hasChanges());

        entry.modified(Arrays.asList("/foo/bar4", "/foo5/bar5"));
        assertEquals("Incorrect number of paths", 10, entry.getNumChangedNodes());
        assertTrue("Incorrect hasChanges", entry.hasChanges());
    }

    @Test
    public void branchAdditionMarksChanges() {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);

        assertFalse("Incorrect hasChanges", entry.hasChanges());

        entry.branchCommit(Collections.<Revision>emptyList());
        assertFalse("Incorrect hasChanges", entry.hasChanges());

        entry.branchCommit(Collections.singleton(Revision.fromString("r123-0-1")));
        assertTrue("Incorrect hasChanges", entry.hasChanges());
    }

    @Test
    public void emptyBranchCommit() {
        DocumentStore store = new MemoryDocumentStore();
        JournalEntry entry = JOURNAL.newDocument(store);

        entry.branchCommit(Collections.<Revision>emptyList());
        assertFalse(entry.getBranchCommits().iterator().hasNext());
        assertNull(entry.get(JournalEntry.BRANCH_COMMITS));
    }

    private static void addRandomPaths(java.util.Collection<String> paths) throws IOException {
        paths.add("/");
        Random random = new Random(42);
        for (int i = 0; i < 1000; i++) {
            String path = "/";
            int depth = random.nextInt(6);
            for (int j = 0; j < depth; j++) {
                char name = (char) ('a' + random.nextInt(26));
                path = PathUtils.concat(path, String.valueOf(name));
                paths.add(path);
            }
        }
    }

    private static void add(StringSort sort, List<String> paths)
            throws IOException {
        for (String p : paths) {
            sort.add(p);
        }
    }

    private static List<String> getChildren(String diff) {
        List<String> children = Lists.newArrayList();
        JsopTokenizer t = new JsopTokenizer(diff);
        for (;;) {
            int r = t.read();
            switch (r) {
                case '^': {
                    children.add(t.readString());
                    t.read(':');
                    t.read('{');
                    t.read('}');
                    break;
                }
                case JsopReader.END: {
                    return children;
                }
                default:
                    fail("Unexpected token: " + r);
            }
        }
    }

    private void validateCacheUsage(DiffCache cache,
                                    RevisionVector from,
                                    RevisionVector to,
                                    String path,
                                    boolean cacheExpected) {
        String nonLoaderDiff = cache.getChanges(from, to, path, null);
        final AtomicBoolean loaderCalled = new AtomicBoolean(false);
        cache.getChanges(from, to, path, new DiffCache.Loader() {
            @Override
            public String call() {
                loaderCalled.set(true);
                return "";
            }
        });

        if (cacheExpected) {
            assertNotNull(nonLoaderDiff);
            assertFalse(loaderCalled.get());
        } else {
            assertNull(nonLoaderDiff);
            assertTrue(loaderCalled.get());
        }
    }

    private static StringSort externalChanges(Revision from,
                                              Revision to,
                                              DocumentStore store)
            throws IOException {
        StringSort changes = JournalEntry.newSorter();
        StringSort invalidate = JournalEntry.newSorter();
        try {
            JournalEntry.fillExternalChanges(changes, invalidate, from, to, store);
        } finally {
            invalidate.close();
        } return changes;
    }
}
