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
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.MoreExecutors;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrefetchDispatcherTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void prefetchVisibleChanges() throws Exception {
        final AtomicInteger numQueries = new AtomicInteger();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                if (collection == Collection.NODES) {
                    numQueries.incrementAndGet();
                }
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(store).setClusterId(1)
                .setPrefetchExternalChanges(false)
                .setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store).setClusterId(2)
                .setPrefetchExternalChanges(false)
                .setAsyncDelay(0).getNodeStore();

        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo").child("bar").child("baz");
        builder.child(":hidden").child("foo").child("bar");
        ns1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        DocumentNodeState before = ns2.getRoot();
        ns2.runBackgroundOperations();
        DocumentNodeState after = ns2.getRoot().fromExternalChange();

        PrefetchDispatcher dispatcher = new PrefetchDispatcher(
                before, MoreExecutors.sameThreadExecutor());
        numQueries.set(0);
        dispatcher.contentChanged(after, CommitInfo.EMPTY_EXTERNAL);
        // expect two queries for children: below /foo and /foo/bar
        assertEquals(2, numQueries.get());
    }
}
