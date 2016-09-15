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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndexTest.createIndex;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HybridIndexTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private NodeStore nodeStore;
    private DocumentQueue queue;
    private Clock clock = new Clock.Virtual();
    private Whiteboard wb;

    private long refreshDelta = TimeUnit.SECONDS.toMillis(1);

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MountInfoProvider mip = defaultMountInfoProvider();

        NRTIndexFactory nrtIndexFactory = new NRTIndexFactory(copier, clock, TimeUnit.MILLISECONDS.toSeconds(refreshDelta));
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);
        IndexTracker tracker = new IndexTracker(indexReaderFactory,nrtIndexFactory);
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker);

        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null,
                null,
                mip);

        queue = new DocumentQueue(100, tracker, sameThreadExecutor());
        LocalIndexObserver localIndexObserver = new LocalIndexObserver(queue, StatisticsProvider.NOOP);

        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(localIndexObserver)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }

    @Test
    public void hybridIndex() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        idx.setProperty(createProperty(IndexConstants.ASYNC_PROPERTY_NAME, ImmutableSet.of("sync" , "async"), STRINGS));
        root.commit();

        //Get initial indexing done as local indexing only work
        //for incremental indexing
        createPath("/a").setProperty("foo", "bar");
        root.commit();

        runAsyncIndex();

        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Add new node. This would not be reflected in result as local index would not be updated
        createPath("/b").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Now let some time elapse such that readers can be refreshed
        clock.waitUntil(clock.getTime() + refreshDelta + 1);

        //Now recently added stuff should be visible without async indexing run
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));

        createPath("/c").setProperty("foo", "bar");
        root.commit();

        //Post async index it should still be upto date
        runAsyncIndex();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b", "/c"));
    }

    @Test
    public void noTextExtractionForSyncCommit() throws Exception{
        String idxName = "hybridtest";
        Tree idx = TestUtil.createFulltextIndex(root.getTree("/"), idxName);
        idx.setProperty(createProperty(IndexConstants.ASYNC_PROPERTY_NAME, ImmutableSet.of("sync" , "async"), STRINGS));
        root.commit();

        runAsyncIndex();

        AccessRecordingBlob testBlob =
                new AccessRecordingBlob("<?xml version=\"1.0\" encoding=\"UTF-8\"?><msg>sky is blue</msg>".getBytes());

        Tree test = root.getTree("/").addChild("test");
        TestUtil.createFileNode(test, "msg", testBlob, "application/xml");
        root.commit();

        assertEquals(0, testBlob.accessCount);
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky')", Collections.<String>emptyList());

        runAsyncIndex();
        assertEquals(1, testBlob.accessCount);
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky')", of("/test/msg/jcr:content"));

    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
        root.refresh();
    }

    private Tree createPath(String path){
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)){
            base = base.addChild(e);
        }
        return base;
    }

    private static class AccessRecordingBlob extends ArrayBasedBlob {
        int accessCount = 0;
        public AccessRecordingBlob(byte[] value) {
            super(value);
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            accessCount++;
            return super.getNewStream();
        }
    }
}
