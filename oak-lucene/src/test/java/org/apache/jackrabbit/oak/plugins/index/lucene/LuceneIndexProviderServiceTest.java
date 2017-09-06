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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.plugins.blob.datastore.CachingFileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.InfoStream;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.ServiceReference;

public class LuceneIndexProviderServiceTest {
    /*
        The test case uses raw config name and not access it via
         constants in LuceneIndexProviderService to ensure that change
         in names are detected
     */

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    private LuceneIndexProviderService service = new LuceneIndexProviderService();

    @Before
    public void setUp(){
        context.registerService(MountInfoProvider.class, Mounts.defaultMountInfoProvider());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        context.registerService(ScorerProviderFactory.class, ScorerProviderFactory.DEFAULT);
        context.registerService(IndexAugmentorFactory.class, new IndexAugmentorFactory());
        context.registerService(NodeStore.class, new MemoryNodeStore());
        context.registerService(IndexPathService.class, mock(IndexPathService.class));
        context.registerService(AsyncIndexInfoService.class, mock(AsyncIndexInfoService.class));
        context.registerService(CheckpointMBean.class, mock(CheckpointMBean.class));
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @After
    public void after(){
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    @Test
    public void defaultSetup() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(Observer.class));
        assertNotNull(context.getService(IndexEditorProvider.class));

        LuceneIndexEditorProvider editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertNotNull(editorProvider.getIndexCopier());
        assertNotNull(editorProvider.getIndexingQueue());

        IndexCopier indexCopier = service.getIndexCopier();
        assertNotNull("IndexCopier should be initialized as CopyOnRead is enabled by default", indexCopier);
        assertTrue(indexCopier.isPrefetchEnabled());
        assertFalse(IndexDefinition.isDisableStoredIndexDefinition());

        assertNotNull("CopyOnRead should be enabled by default", context.getService(CopyOnReadStatsMBean.class));
        assertNotNull(context.getService(CacheStatsMBean.class));

        assertTrue(context.getService(Observer.class) instanceof BackgroundObserver);
        assertEquals(InfoStream.NO_OUTPUT, InfoStream.getDefault());

        assertEquals(1024, BooleanQuery.getMaxClauseCount());

        assertNotNull(FieldUtils.readDeclaredField(service, "documentQueue", true));

        assertNotNull(context.getService(JournalPropertyService.class));
        assertNotNull(context.getService(IndexImporterProvider.class));

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void typeProperty() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
        ServiceReference sr = context.bundleContext().getServiceReference(IndexEditorProvider.class.getName());
        assertEquals("lucene", sr.getProperty("type"));
    }

    @Test
    public void disableOpenIndexAsync() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("enableOpenIndexAsync", false);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertTrue(context.getService(Observer.class) instanceof LuceneIndexProvider);

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void enableCopyOnWrite() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("enableCopyOnWriteSupport", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        LuceneIndexEditorProvider editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);

        assertNotNull(editorProvider);
        assertNotNull(editorProvider.getIndexCopier());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void enablePrefetchIndexFiles() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("prefetchIndexFiles", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        IndexCopier indexCopier = service.getIndexCopier();
        assertTrue(indexCopier.isPrefetchEnabled());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void debugLogging() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("debug", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertEquals(LoggingInfoStream.INSTANCE, InfoStream.getDefault());
        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void enableExtractedTextCaching() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("extractedTextCacheSizeInMB", 11);
        MockOsgi.activate(service, context.bundleContext(), config);

        ExtractedTextCache textCache = service.getExtractedTextCache();
        assertNotNull(textCache.getCacheStats());
        assertNotNull(context.getService(CacheStatsMBean.class));

        assertEquals(11 * FileUtils.ONE_MB, textCache.getCacheStats().getMaxTotalWeight());

        MockOsgi.deactivate(service, context.bundleContext());

        assertNull(context.getService(CacheStatsMBean.class));
    }

    @Test
    public void preExtractedTextProvider() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
        LuceneIndexEditorProvider editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertNull(editorProvider.getExtractedTextCache().getExtractedTextProvider());
        assertFalse(editorProvider.getExtractedTextCache().isAlwaysUsePreExtractedCache());

        //Mock OSGi does not support components
        //context.registerService(PreExtractedTextProvider.class, new DummyProvider());
        service.bindExtractedTextProvider(mock(PreExtractedTextProvider.class));

        assertNotNull(editorProvider.getExtractedTextCache().getExtractedTextProvider());
    }

    @Test
    public void preExtractedProviderBindBeforeActivate() throws Exception{
        service.bindExtractedTextProvider(mock(PreExtractedTextProvider.class));
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
        LuceneIndexEditorProvider editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertNotNull(editorProvider.getExtractedTextCache().getExtractedTextProvider());
    }

    @Test
    public void alwaysUsePreExtractedCache() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("alwaysUsePreExtractedCache", "true");
        MockOsgi.activate(service, context.bundleContext(), config);
        LuceneIndexEditorProvider editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertTrue(editorProvider.getExtractedTextCache().isAlwaysUsePreExtractedCache());
    }

    @Test
    public void booleanQuerySize() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("booleanClauseLimit", 4000);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertEquals(4000, BooleanQuery.getMaxClauseCount());
    }

    @Test
    public void indexDefnStorafe() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("disableStoredIndexDefinition", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertTrue(IndexDefinition.isDisableStoredIndexDefinition());
    }


    @Test
    public void blobStoreRegistered() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
        LuceneIndexEditorProvider editorProvider =
            (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertNull(editorProvider.getBlobStore());

        /* Register a blob store */
        CachingFileDataStore ds = DataStoreUtils
            .createCachingFDS(folder.newFolder().getAbsolutePath(),
                folder.newFolder().getAbsolutePath());

        context.registerService(GarbageCollectableBlobStore.class, new DataStoreBlobStore(ds));
        reactivate();

        editorProvider =
                (LuceneIndexEditorProvider) context.getService(IndexEditorProvider.class);
        assertNotNull(editorProvider.getBlobStore());
    }

    @Test
    public void executorPoolBehaviour() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
        ExecutorService executor = service.getExecutorService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        Callable cb1 = new Callable() {
            @Override
            public Object call() throws Exception {
                latch1.await();
                return null;
            }
        };

        Callable cb2 = new Callable() {
            @Override
            public Object call() throws Exception {
                latch2.countDown();
                return null;
            }
        };

        executor.submit(cb1);
        executor.submit(cb2);

        //Even if one task gets stuck the other task must get completed
        assertTrue("Second task not executed", latch2.await(1, TimeUnit.MINUTES));
        latch1.countDown();

        MockOsgi.deactivate(service, context.bundleContext());
    }



    private void reactivate() {
        MockOsgi.deactivate(service, context.bundleContext());
        service = new LuceneIndexProviderService();

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());
    }

    private Map<String,Object> getDefaultConfig(){
        Map<String,Object> config = new HashMap<String, Object>();
        config.put("localIndexDir", folder.getRoot().getAbsolutePath());
        return config;
    }
}
