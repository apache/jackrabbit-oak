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

import org.apache.jackrabbit.guava.common.collect.HashMultimap;

import org.apache.jackrabbit.guava.common.collect.Multimap;
import org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.stats.StatisticsProvider.NOOP;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

public class ExternalIndexObserverTest {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private IndexingQueue queue;
    @Mock
    private IndexTracker tracker;

    private ExternalIndexObserver observer;

    private CommitContext commitContext = new SimpleCommitContext();

    @Before
    public void setUp() {
        observer = new ExternalIndexObserver(queue, tracker, NOOP);
    }

    @Test
    public void internalChange() throws Exception {
        observer.contentChanged(INITIAL_CONTENT, CommitInfo.EMPTY);
        verifyNoInteractions(queue);
    }

    @Test
    public void externalChangeNullContext() throws Exception {
        observer.contentChanged(INITIAL_CONTENT, CommitInfo.EMPTY_EXTERNAL);
        verifyNoInteractions(queue);
    }

    @Test
    public void emptyCommitContext() throws Exception {
        CommitInfo ci = newCommitInfo();
        observer.contentChanged(INITIAL_CONTENT, ci);
        verifyNoInteractions(queue);
    }

    @Test
    public void nonExistingIndexDefn() throws Exception {
        Multimap<String, String> indexedPaths = HashMultimap.create();
        indexedPaths.put("/a", "/oak:index/foo");

        commitContext.set(LuceneDocumentHolder.NAME, new IndexedPaths(indexedPaths));

        CommitInfo ci = newCommitInfo();

        observer.contentChanged(INITIAL_CONTENT, ci);
        verifyNoInteractions(queue);
    }

    @Test
    public void nonExistingPath() throws Exception {
        Multimap<String, String> indexedPaths = HashMultimap.create();
        indexedPaths.put("/a", "/oak:index/foo");

        commitContext.set(LuceneDocumentHolder.NAME, new IndexedPaths(indexedPaths));

        CommitInfo ci = newCommitInfo();
        when(tracker.getIndexDefinition("/oak:index/foo")).thenReturn(createNRTIndex("nt:base"));
        observer.contentChanged(INITIAL_CONTENT, ci);
        verifyNoInteractions(queue);
    }

    @Test
    public void nonApplicableRule() throws Exception {
        Multimap<String, String> indexedPaths = HashMultimap.create();
        indexedPaths.put("/a", "/oak:index/foo");

        commitContext.set(LuceneDocumentHolder.NAME, new IndexedPaths(indexedPaths));
        CommitInfo ci = newCommitInfo();

        //Rule is on nt:file but node if of type nt:base
        when(tracker.getIndexDefinition("/oak:index/foo")).thenReturn(createNRTIndex("nt:file"));

        NodeBuilder nb = INITIAL_CONTENT.builder();
        nb.child("a");
        observer.contentChanged(nb.getNodeState(), ci);

        verifyNoInteractions(queue);
    }

    @Test
    public void ruleNotResultingInDoc() throws Exception {
        Multimap<String, String> indexedPaths = HashMultimap.create();
        indexedPaths.put("/a", "/oak:index/foo");

        commitContext.set(LuceneDocumentHolder.NAME, new IndexedPaths(indexedPaths));
        CommitInfo ci = newCommitInfo();

        //Rule is of type nt:base but does not have any matching property definition
        when(tracker.getIndexDefinition("/oak:index/foo")).thenReturn(createNRTIndex("nt:base"));

        NodeBuilder nb = INITIAL_CONTENT.builder();
        nb.child("a");
        observer.contentChanged(nb.getNodeState(), ci);

        verifyNoInteractions(queue);
    }

    @Test
    public void docAddedToQueue() throws Exception {
        assertIndexing(observer);
    }

    private void assertIndexing(Observer observer){
        Multimap<String, String> indexedPaths = HashMultimap.create();
        indexedPaths.put("/a", "/oak:index/foo");

        commitContext.set(LuceneDocumentHolder.NAME, new IndexedPaths(indexedPaths));
        CommitInfo ci = newCommitInfo();

        when(queue.add(any(LuceneDoc.class))).thenReturn(true);
        when(tracker.getIndexDefinition("/oak:index/foo")).thenReturn(createNRTIndex("nt:base"));

        NodeBuilder nb = INITIAL_CONTENT.builder();
        nb.child("a").setProperty("foo", "bar");
        observer.contentChanged(nb.getNodeState(), ci);

        ArgumentCaptor<LuceneDoc> doc = ArgumentCaptor.forClass(LuceneDoc.class);
        verify(queue).add(doc.capture());

        assertEquals("/oak:index/foo", doc.getValue().getIndexPath());
    }

    @Test
    public void builder() throws Exception{
        ExternalObserverBuilder builder =
                new ExternalObserverBuilder(queue, tracker,NOOP, MoreExecutors.newDirectExecutorService(), 10);
        Observer o = builder.build();
        o.contentChanged(INITIAL_CONTENT, CommitInfo.EMPTY_EXTERNAL);
        verifyNoInteractions(queue);
    }

    @Test
    public void builder_NonFiltered() throws Exception{
        ExternalObserverBuilder builder =
                new ExternalObserverBuilder(queue, tracker,NOOP, MoreExecutors.newDirectExecutorService(), 10);
        assertIndexing(builder.build());
    }

    private CommitInfo newCommitInfo() {
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN,
                Map.of(CommitContext.NAME, commitContext), true);
    }

    private static LuceneIndexDefinition createNRTIndex(String ruleName) {
        LuceneIndexDefinitionBuilder idx = new LuceneIndexDefinitionBuilder();
        idx.indexRule(ruleName).property("foo").propertyIndex();
        idx.async("async", "sync");
        return new LuceneIndexDefinition(INITIAL_CONTENT, idx.build(), "/oak:index/foo");
    }

}
