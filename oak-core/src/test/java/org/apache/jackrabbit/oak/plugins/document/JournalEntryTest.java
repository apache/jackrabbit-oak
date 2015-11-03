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
        Revision from = new Revision(1, 0, 1);
        Revision to = new Revision(2, 0, 1);
        sort.sort();
        JournalEntry.applyTo(sort, cache, from, to);

        for (String p : paths) {
            String changes = cache.getChanges(from, to, p, null);
            assertNotNull("missing changes for " + p, changes);
            for (String c : getChildren(changes)) {
                assertTrue(paths.contains(PathUtils.concat(p, c)));
            }
        }
        sort.close();
    }

    //OAK-3494
    @Test
    public void useParentDiff() throws Exception {
        DiffCache cache = new MemoryDiffCache(new DocumentMK.Builder());
        Revision from = new Revision(1, 0, 1);
        Revision to = new Revision(2, 0, 1);
        Revision unjournalled = new Revision(3, 0, 1);

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
        JournalEntry.applyTo(sort, cache, from, to);

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
        JournalEntry.fillExternalChanges(sort, r2, r3, store);
        assertEquals(0, sort.getSize());

        JournalEntry.fillExternalChanges(sort, r1, r2, store);
        assertEquals(paths.size(), sort.getSize());
        sort.close();

        sort = JournalEntry.newSorter();
        JournalEntry.fillExternalChanges(sort, r1, r3, store);
        assertEquals(paths.size(), sort.getSize());
        sort.close();
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

    private void validateCacheUsage(DiffCache cache, Revision from, Revision to, String path, boolean cacheExpected) {
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
}
