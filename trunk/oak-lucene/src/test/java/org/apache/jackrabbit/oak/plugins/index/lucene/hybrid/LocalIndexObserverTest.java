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

import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LocalIndexObserverTest {
    static final Executor NOOP_EXECUTOR = new Executor() {
        @Override
        public void execute(Runnable command) {

        }
    };

    private IndexTracker tracker = new IndexTracker();
    private DocumentQueue collectingQueue;
    private LocalIndexObserver observer;

    @Before
    public void setUp(){
        collectingQueue = new DocumentQueue(10, tracker, NOOP_EXECUTOR);
        observer = new LocalIndexObserver(collectingQueue, StatisticsProvider.NOOP);
    }

    @Test
    public void externalCommitInfo() throws Exception{
        observer.contentChanged(EMPTY_NODE, CommitInfo.EMPTY_EXTERNAL);
    }

    @Test
    public void noCommitContext() throws Exception{
        observer.contentChanged(EMPTY_NODE, CommitInfo.EMPTY);
    }

    @Test
    public void noDocHolder() throws Exception{
        observer.contentChanged(EMPTY_NODE, newCommitInfo());
    }

    @Test
    public void docsAddedToQueue() throws Exception{
        CommitInfo info = newCommitInfo();
        CommitContext cc = (CommitContext) info.getInfo().get(CommitContext.NAME);

        LuceneDocumentHolder holder = new LuceneDocumentHolder(collectingQueue, 500);
        holder.add(false, LuceneDoc.forDelete("foo", "bar"));

        cc.set(LuceneDocumentHolder.NAME, holder);

        observer.contentChanged(EMPTY_NODE, info);

        assertEquals(1, collectingQueue.getQueuedDocs().size());
        assertNull(cc.get(LuceneDocumentHolder.NAME));
    }

    private CommitInfo newCommitInfo(){
        return new CommitInfo("admin", "s1",
                ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext()));
    }


}