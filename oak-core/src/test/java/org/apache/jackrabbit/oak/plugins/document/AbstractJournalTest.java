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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

/**
 * Base class for journal related tests.
 */
public abstract class AbstractJournalTest {

    protected TestBuilder builder;
    protected List<DocumentMK> mks = Lists.newArrayList();
    protected Random random;

    @Before
    public void setup() {
        random = new Random();
    }

    @Before
    @After
    public void clear() {
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        mks.clear();
    }

    protected static void renewClusterIdLease(DocumentNodeStore store) {
        store.renewClusterIdLease();
    }

    protected Set<String> choose(List<String> paths, int howMany) {
        final Set<String> result = new HashSet<String>();
        while(result.size()<howMany) {
            result.add(paths.get(random.nextInt(paths.size())));
        }
        return result;
    }

    protected List<String> createRandomPaths(int depth, int avgChildrenPerLevel, int num) {
        final Set<String> result = new HashSet<String>();
        while(result.size()<num) {
            result.add(createRandomPath(depth, avgChildrenPerLevel));
        }
        return new ArrayList<String>(result);
    }

    protected String createRandomPath(int depth, int avgChildrenPerLevel) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<depth; i++) {
            sb.append("/");
            sb.append("r").append(random.nextInt(avgChildrenPerLevel));
        }
        return sb.toString();
    }

    protected void assertDocCache(DocumentNodeStore ns, boolean expected, String path) {
        String id = Utils.getIdFromPath(path);
        boolean exists = ns.getDocumentStore().getIfCached(Collection.NODES, id)!=null;
        if (exists!=expected) {
            if (expected) {
                fail("assertDocCache: did not find in cache even though expected: "+path);
            } else {
                fail("assertDocCache: found in cache even though not expected: "+path);
            }
        }
    }

    protected void setProperty(DocumentNodeStore ns, String path, String key, String value, boolean runBgOpsAfterCreation) throws
            CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        doGetOrCreate(rootBuilder, path).setProperty(key, value);
        ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        if (runBgOpsAfterCreation) {
            ns.runBackgroundOperations();
        }
    }

    protected void getOrCreate(DocumentNodeStore ns, List<String> paths, boolean runBgOpsAfterCreation) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        for(String path:paths) {
            doGetOrCreate(rootBuilder, path);
        }
        ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        if (runBgOpsAfterCreation) {
            ns.runBackgroundOperations();
        }
    }

    protected void getOrCreate(DocumentNodeStore ns, String path, boolean runBgOpsAfterCreation) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        doGetOrCreate(rootBuilder, path);
        ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        if (runBgOpsAfterCreation) {
            ns.runBackgroundOperations();
        }
    }

    protected NodeBuilder doGetOrCreate(NodeBuilder builder, String path) {
        String[] parts = path.split("/");
        for(int i=1; i<parts.length; i++) {
            builder = builder.child(parts[i]);
        }
        return builder;
    }

    protected void assertJournalEntries(DocumentNodeStore ds, String... expectedChanges) {
        List<String> exp = new LinkedList<String>(asList(expectedChanges));
        for(boolean branch : new Boolean[]{false, true}) {
            String fromKey = JournalEntry.asId(new Revision(0, 0, ds.getClusterId(), branch));
            String toKey = JournalEntry.asId(new Revision(System.currentTimeMillis()+1000, 0, ds.getClusterId(), branch));
            List<JournalEntry> entries = ds.getDocumentStore().query(Collection.JOURNAL, fromKey, toKey, expectedChanges.length+5);
            if (entries.size()>0) {
                for (JournalEntry journalEntry : entries) {
                    if (!exp.remove(journalEntry.get("_c"))) {
                        fail("Found an unexpected change: " + journalEntry.get("_c") + ", while all I expected was: " + asList(expectedChanges));
                    }
                }
            }
        }
        if (exp.size()>0) {
            fail("Did not find all expected changes, left over: "+exp+" (from original list which is: "+asList(expectedChanges)+")");
        }
    }

    protected int countJournalEntries(DocumentNodeStore ds, int max) {
        int total = 0;
        for(boolean branch : new Boolean[]{false, true}) {
            String fromKey = JournalEntry.asId(new Revision(0, 0, ds.getClusterId(), branch));
            String toKey = JournalEntry.asId(new Revision(System.currentTimeMillis()+1000, 0, ds.getClusterId(), branch));
            List<JournalEntry> entries = ds.getDocumentStore().query(Collection.JOURNAL, fromKey, toKey, max);
            total+=entries.size();
        }
        return total;
    }

    protected NodeDocument getDocument(DocumentNodeStore nodeStore, String path) {
        return nodeStore.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
    }

    protected TestBuilder newDocumentMKBuilder() {
        return new TestBuilder();
    }

    protected DocumentMK createMK(int clusterId, int asyncDelay,
                                  DocumentStore ds, BlobStore bs) {
        builder = newDocumentMKBuilder();
        return register(builder.setDocumentStore(ds)
                .setBlobStore(bs).setClusterId(clusterId)
                .setAsyncDelay(asyncDelay).open());
    }

    protected DocumentMK register(DocumentMK mk) {
        mks.add(mk);
        return mk;
    }

    protected final class TestBuilder extends DocumentMK.Builder {
        CountingDocumentStore actualStore;
        CountingTieredDiffCache actualDiffCache;

        @Override
        public DocumentStore getDocumentStore() {
            if (actualStore==null) {
                actualStore = new CountingDocumentStore(super.getDocumentStore());
            }
            return actualStore;
        }

        @Override
        public DiffCache getDiffCache() {
            if (actualDiffCache==null) {
                actualDiffCache = new CountingTieredDiffCache(this);
            }
            return actualDiffCache;
        }
    }
}
