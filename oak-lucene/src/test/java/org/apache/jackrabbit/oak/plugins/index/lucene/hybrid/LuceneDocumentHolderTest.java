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
package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.ListValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

public class LuceneDocumentHolderTest {
    private DummyQueue queue = new DummyQueue();
    private LuceneDocumentHolder holder = new LuceneDocumentHolder(queue, 100);

    @Test
    public void testAllLuceneDocReturned() throws Exception{
        queue.enabled = false;
        holder.add(true, LuceneDoc.forDelete("/oak:index/foo", "/a"));
        holder.add(false, LuceneDoc.forDelete("/oak:index/bar", "/b"));

        queue.enabled = true;
        holder.add(true, LuceneDoc.forDelete("/oak:index/foo", "/c"));
        holder.add(false, LuceneDoc.forDelete("/oak:index/bar", "/d"));

        assertThat(asMultiMap(holder.getAllLuceneDocInfo()).values(), hasItems("/a", "/b", "/c", "/d"));
    }

    @Test
    public void unprocessedSyncQueuedDocs() throws Exception{
        queue.enabled = true;
        holder.add(true, LuceneDoc.forDelete("/oak:index/foo", "/a"));
        holder.add(true, LuceneDoc.forDelete("/oak:index/foo", "/c"));

        queue.enabled = false;
        holder.add(true, LuceneDoc.forDelete("/oak:index/foo", "/b"));

        queue.luceneDocs.get("/c").markProcessed();

        assertThat(asMultiMap(holder.getSyncIndexedDocs()).values(), containsInAnyOrder("/a", "/b"));
    }

    private static ListValuedMap<String, String> asMultiMap(Iterable<? extends LuceneDocInfo> itr){
        ListValuedMap<String, String> docs = new ArrayListValuedHashMap<>();
        for (LuceneDocInfo d : itr){
            docs.put(d.getIndexPath(), d.getDocPath());
        }
        return docs;
    }

    private static ListValuedMap<String, String> asMultiMap(Map<String, Collection<LuceneDoc>> map){
        ListValuedMap<String, String> docs = new ArrayListValuedHashMap<>();
        for (Collection<LuceneDoc> lds : map.values()){
            for (LuceneDoc d : lds){
                docs.put(d.getIndexPath(), d.getDocPath());
            }
        }
        return docs;
    }

    private static class DummyQueue implements IndexingQueue {
        boolean enabled;
        ListValuedMap<String, String> docs = new ArrayListValuedHashMap<>();
        Map<String, LuceneDoc> luceneDocs = new HashMap<>();

        @Override
        public boolean addIfNotFullWithoutWait(LuceneDoc doc) {
            if (enabled){
                docs.put(doc.indexPath, doc.docPath);
                luceneDocs.put(doc.docPath, doc);
            }
            return enabled;
        }

        @Override
        public boolean add(LuceneDoc doc) {
            throw new AbstractMethodError();
        }

        @Override
        public void addAllSynchronously(Map<String, Collection<LuceneDoc>> docsPerIndex) {
            throw new AbstractMethodError();
        }

        @Override
        public void scheduleQueuedDocsProcessing() {

        }
    }
}
