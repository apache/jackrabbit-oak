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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.InfoStream;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
        context.registerService(ScorerProviderFactory.class, ScorerProviderFactory.DEFAULT);
        context.registerService(IndexAugmentorFactory.class, mock(IndexAugmentorFactory.class));
        MockOsgi.injectServices(service, context.bundleContext());
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

        IndexCopier indexCopier = service.getIndexCopier();
        assertNotNull("IndexCopier should be initialized as CopyOnRead is enabled by default", indexCopier);
        assertTrue(indexCopier.isPrefetchEnabled());

        assertNotNull("CopyOnRead should be enabled by default", context.getService(CopyOnReadStatsMBean.class));
        assertNotNull(context.getService(CacheStatsMBean.class));

        assertTrue(context.getService(Observer.class) instanceof BackgroundObserver);
        assertEquals(InfoStream.NO_OUTPUT, InfoStream.getDefault());

        assertEquals(1024, BooleanQuery.getMaxClauseCount());

        MockOsgi.deactivate(service);
    }

    @Test
    public void disableOpenIndexAsync() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("enableOpenIndexAsync", false);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertTrue(context.getService(Observer.class) instanceof LuceneIndexProvider);

        MockOsgi.deactivate(service);
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

        MockOsgi.deactivate(service);
    }

    @Test
    public void enablePrefetchIndexFiles() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("prefetchIndexFiles", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        IndexCopier indexCopier = service.getIndexCopier();
        assertTrue(indexCopier.isPrefetchEnabled());

        MockOsgi.deactivate(service);
    }

    @Test
    public void debugLogging() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("debug", true);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertEquals(LoggingInfoStream.INSTANCE, InfoStream.getDefault());
        MockOsgi.deactivate(service);
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

        MockOsgi.deactivate(service);

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
        service.bindExtractedTextProvider(new DummyProvider());

        assertNotNull(editorProvider.getExtractedTextCache().getExtractedTextProvider());
    }

    @Test
    public void preExtractedProviderBindBeforeActivate() throws Exception{
        service.bindExtractedTextProvider(new DummyProvider());
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

    private Map<String,Object> getDefaultConfig(){
        Map<String,Object> config = new HashMap<String, Object>();
        config.put("localIndexDir", folder.getRoot().getAbsolutePath());
        return config;
    }

    private static class DummyProvider implements PreExtractedTextProvider {

        @Override
        public ExtractedText getText(String propertyPath, Blob blob) throws IOException {
            return null;
        }
    }
}
