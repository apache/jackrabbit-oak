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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneJournalPropertyBuilderTest {

    private LuceneJournalPropertyBuilder builder = new LuceneJournalPropertyBuilder(1000);

    @Test
    public void nullProperty() throws Exception{
        builder.addProperty(null);
        assertEquals("{}", builder.buildAsString());
        assertTrue(Iterables.isEmpty(((IndexedPaths)builder.build())));
    }

    @Test
    public void nullOrEmptyJson() throws Exception{
        builder.addProperty(null);

        LuceneJournalPropertyBuilder builder2 = new LuceneJournalPropertyBuilder(1000);
        builder2.addSerializedProperty(null);
        builder2.addSerializedProperty(builder.buildAsString());

        assertTrue(Iterables.isEmpty(((IndexedPaths)builder2.build())));
    }

    @Test
    public void addMulti() throws Exception{
        LuceneDocumentHolder h1 = createHolder();
        h1.add(true, LuceneDoc.forDelete("/oak:index/foo", "/a"));
        h1.add(true, LuceneDoc.forDelete("/oak:index/foo", "/b"));
        builder.addProperty(h1);

        LuceneDocumentHolder h2 = createHolder();
        h2.add(true, LuceneDoc.forDelete("/oak:index/bar", "/a"));
        builder.addProperty(h2);

        IndexedPaths indexedPaths = (IndexedPaths) builder.build();
        Multimap<String, String> map = createdIndexPathMap(indexedPaths);
        assertThat(map.keySet(), containsInAnyOrder("/oak:index/foo", "/oak:index/bar"));
        assertThat(map.get("/oak:index/foo"), containsInAnyOrder("/a", "/b"));
    }

    @Test
    public void addMultiJson() throws Exception{
        LuceneDocumentHolder h1 = createHolder();
        h1.add(true, LuceneDoc.forDelete("/oak:index/foo", "/a"));
        h1.add(true, LuceneDoc.forDelete("/oak:index/foo", "/b"));
        builder.addProperty(h1);

        LuceneDocumentHolder h2 = createHolder();
        h2.add(true, LuceneDoc.forDelete("/oak:index/bar", "/a"));
        builder.addProperty(h2);

        String json = builder.buildAsString();
        LuceneJournalPropertyBuilder builder2 = new LuceneJournalPropertyBuilder(1000);
        builder2.addSerializedProperty(json);

        IndexedPaths indexedPaths = (IndexedPaths) builder2.build();
        Multimap<String, String> map = createdIndexPathMap(indexedPaths);
        assertThat(map.keySet(), containsInAnyOrder("/oak:index/foo", "/oak:index/bar"));
        assertThat(map.get("/oak:index/foo"), containsInAnyOrder("/a", "/b"));
    }

    @Test
    public void maxLimitReached() throws Exception{
        int maxSize = 5;
        builder = new LuceneJournalPropertyBuilder(maxSize);
        for (int i = 0; i < maxSize*2; i++) {
            LuceneDocumentHolder h1 = createHolder();
            h1.add(true, LuceneDoc.forDelete("/oak:index/foo", "/a"+i));
            builder.addProperty(h1);
        }

        IndexedPaths indexedPaths = (IndexedPaths) builder.build();
        assertEquals(maxSize, Iterables.size(indexedPaths));
    }

    private Multimap<String, String> createdIndexPathMap(Iterable<IndexedPathInfo> itr){
        Multimap<String, String> map = HashMultimap.create();
        for (IndexedPathInfo i : itr){
            for (String indexPath : i.getIndexPaths()){
                map.put(indexPath, i.getPath());
            }
        }
        return map;
    }

    private LuceneDocumentHolder createHolder(){
        IndexingQueue queue = mock(IndexingQueue.class);
        when(queue.addIfNotFullWithoutWait(any(LuceneDoc.class))).thenReturn(true);
        return new LuceneDocumentHolder(queue, 100);
    }
}