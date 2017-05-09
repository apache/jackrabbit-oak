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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.fixture.DocumentMemoryFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.cluster.AbstractClusterTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.JournalPropertyHandlerFactory;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class HybridIndexClusterIT extends AbstractClusterTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private MemoryDocumentStore documentStore = new MemoryDocumentStore();
    private Whiteboard nswb1 = new DefaultWhiteboard();
    private Whiteboard nswb2 = new DefaultWhiteboard();
    private Clock clock = new Clock.Virtual();
    private long refreshDelta = TimeUnit.SECONDS.toMillis(1);

    @Override
    protected Jcr customize(Jcr jcr) {
        IndexCopier copier;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MountInfoProvider mip = defaultMountInfoProvider();

        NRTIndexFactory nrtIndexFactory = new NRTIndexFactory(copier, clock, TimeUnit.MILLISECONDS.toSeconds(refreshDelta), StatisticsProvider.NOOP);
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);
        IndexTracker tracker = new IndexTracker(indexReaderFactory,nrtIndexFactory);
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker);
        DocumentQueue queue = new DocumentQueue(100, tracker, sameThreadExecutor());
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null,
                null,
                mip);
        editorProvider.setIndexingQueue(queue);

        LocalIndexObserver localIndexObserver = new LocalIndexObserver(queue, StatisticsProvider.NOOP);
        ExternalIndexObserver externalIndexObserver = new ExternalIndexObserver(queue, tracker, StatisticsProvider.NOOP);

        QueryEngineSettings qs = new QueryEngineSettings();
        qs.setFailTraversal(true);

        jcr.with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(localIndexObserver)
                .with(externalIndexObserver)
                .with(editorProvider)
                .with(qs)
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));
        return jcr;
    }

    @Override
    protected NodeStoreFixture getFixture() {
        return new DocumentMemoryFixture(){
            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                JournalPropertyHandlerFactory tracker = new JournalPropertyHandlerFactory();
                Whiteboard wb = clusterNodeId == 1 ? nswb1 : nswb2;
                tracker.start(wb);
                return new DocumentMK.Builder()
                        .setDocumentStore(documentStore)
                        .setJournalPropertyHandlerFactory(tracker)
                        .setAsyncDelay(0)
                        .getNodeStore();
            }
        };
    }

    @Override
    protected void prepareTestData(Session s) throws RepositoryException {
        nswb1.register(JournalPropertyService.class, new LuceneJournalPropertyService(1000), null);
        nswb2.register(JournalPropertyService.class, new LuceneJournalPropertyService(1000), null);

        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("nt:base").property("foo").propertyIndex();
        idx.async("async", "sync");

        Node idxNode = JcrUtils.getOrCreateByPath("/oak:index/fooIndex", "oak:QueryIndexDefinition", s);
        idx.build(idxNode);

        Node a = JcrUtils.getOrCreateByPath("/a", "oak:Unstructured", s);
        a.setProperty("foo", "x");
        s.save();
        runAsyncIndex(w1);
    }

    @Test
    public void basics() throws Exception{
        assertThat(queryResult(s1, "foo", "x"), containsInAnyOrder("/a"));
        assertThat(queryResult(s2, "foo", "x"), containsInAnyOrder("/a"));
    }

    @Test
    public void indexExternalChange() throws Exception{
        Node a = JcrUtils.getOrCreateByPath("/b", "oak:Unstructured", s1);
        a.setProperty("foo", "x");
        s1.save();

        assertThat(queryResult(s1, "foo", "x"), containsInAnyOrder("/a", "/b"));
        //on C2 /b would not be part of index
        assertThat(queryResult(s2, "foo", "x"), containsInAnyOrder("/a"));

        asDS(ns1).runBackgroundOperations();
        asDS(ns2).runBackgroundOperations();

        //Now the external change should be picked up and reflect in query result
        assertThat(queryResult(s2, "foo", "x"), containsInAnyOrder("/a", "/b"));
    }

    private static DocumentNodeStore asDS(NodeStore ns){
        return (DocumentNodeStore)ns;
    }

    private static void runAsyncIndex(Whiteboard wb) {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
    }

    private static List<String> queryResult(Session session, String indexedPropName, String value) throws RepositoryException{
        session.refresh(false);
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("select * from [nt:base] where [" + indexedPropName + "] = $value", Query.JCR_SQL2);
        q.bindValue("value", session.getValueFactory().createValue(value));
        QueryResult result = q.execute();
        List<String> paths = Lists.newArrayList();
        for (Row r : JcrUtils.getRows(result)){
            paths.add(r.getPath());
        }
        return paths;
    }
}
