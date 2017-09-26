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

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.matchers.JUnitMatchers.hasItem;


public class OptimizedChildFetchTest extends BaseDocumentMKTest {

    private TestDocumentStore ds = new TestDocumentStore();

    @Before
    public void initDocumentMK() {
        mk = new DocumentMK.Builder().setDocumentStore(ds).open();
    }

    @Test
    public void checkForChildStatusFlag() {
        String head = mk.getHeadRevision();
        mk.commit("",
                "+\"/root\":{}\n" +
                        "+\"/root/a\":{}\n" +
                        "+\"/root/a/b\":{}\n",
                head, "");

        assertTrue(hasChildren("/root"));
        assertTrue(hasChildren("/root/a"));
        assertFalse(hasChildren("/root/a/b"));
    }

    @Test
    public void checkForNoCallsToFetchChildForLeafNodes() {
        String head = mk.getHeadRevision();
        String rev = mk.commit("",
                "+\"/root\":{}\n" +
                        "+\"/root/a\":{}\n" +
                        "+\"/root/c\":{}\n" +
                        "+\"/root/a/b\":{}\n",
                head, "");

        //Clear the caches
        ds.paths.clear();
        resetMK();

        //Check that call is made to fetch child for non
        //leaf nodes
        mk.getNodes("/root/a", rev, 0, 0, 10, null);
        assertThat(ds.paths, hasItem("3:/root/a/"));

        resetMK();
        ds.paths.clear();

        //Check that no query is made to fetch children for
        //leaf nodes
        assertNotNull(mk.getNodes("/root/c", rev, 0, 0, 10, null));
        assertNotNull(mk.getNodes("/root/a/b", rev, 0, 0, 10, null));
        assertTrue(ds.paths.isEmpty());
    }

    private void resetMK() {
        disposeDocumentMK();
        initDocumentMK();

    }

    private boolean hasChildren(String path) {
        NodeDocument nd = mk.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
        return nd.hasChildren();
    }


    private static class TestDocumentStore extends MemoryDocumentStore {
        Set<String> paths = Sets.newHashSet();

        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
                                                  String indexedProperty, long startValue, int limit) {
            paths.add(fromKey);
            return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        }
    }
}
