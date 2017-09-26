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

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.version.VersionablePathHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * <code>VisibleChangesTest</code>...
 */
public class VisibleChangesTest {

    // OAK-3019
    @Test
    public void versionablePathHook() throws CommitFailedException {
        TestStore store = new TestStore();
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(":hidden");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        builder = ns.getRoot().builder();
        NodeBuilder hidden = builder.child(":hidden");

        for (int i = 0; i <= DocumentMK.MANY_CHILDREN_THRESHOLD; i++) {
            hidden.child("child-" + i);
        }
        // add more changes until a branch is created
        NodeBuilder foo = builder.child("foo");
        int numRevs = getRevisionsSize(store, "/");
        while (numRevs == getRevisionsSize(store, "/")) {
            foo.setProperty("p", "v");
            foo.removeProperty("p");
        }

        store.paths.clear();
        VersionablePathHook hook = new VersionablePathHook("default");
        hook.processCommit(ns.getRoot(), builder.getNodeState(), CommitInfo.EMPTY);
        assertEquals("Must not query for hidden paths: " + store.paths.toString(),
                0, store.paths.size());

        ns.dispose();
    }

    private static int getRevisionsSize(DocumentStore store, String path) {
        NodeDocument doc = store.find(Collection.NODES, getIdFromPath(path));
        assertNotNull(doc);
        return doc.getLocalRevisions().size();
    }

    private static final class TestStore extends MemoryDocumentStore {

        private final Set<String> paths = Sets.newHashSet();

        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection,
                                                  String fromKey,
                                                  String toKey,
                                                  String indexedProperty,
                                                  long startValue,
                                                  int limit) {
            if (indexedProperty != null) {
                paths.add(fromKey);
            }
            return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        }
    }
}
