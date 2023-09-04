/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.dynamicBoost;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.DynamicBoostCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexAugmentorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class LuceneDynamicBoostTest extends DynamicBoostCommonTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private final SimpleIndexAugmentorFactory factory = new SimpleIndexAugmentorFactory();

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        indexOptions = new LuceneIndexOptions();
        IndexTracker tracker = new IndexTracker();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null, new ExtractedTextCache(0, 0), factory,
                Mounts.defaultMountInfoProvider());
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker, factory);
        return new Oak().with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    @Override
    protected String getTestQueryDynamicBoostBasicExplained() {
        return "[dam:Asset] as [a] /* lucene:test-index(/oak:index/test-index) (full:title:plant :fulltext:plant) " +
                "((jcr:content/metadata/predictedTags/plant:1 jcr:content/metadata/predictedTags/plant:1)^1.0E-4) ft:(\"plant\")\n" +
                "  where contains([a].[*], 'plant') */";
    }

    @Override
    protected boolean areAnalyzeFeaturesSupportedInLiteModeOnly() {
        return true;
    }

    @Test
    public void indexingFieldProvider() throws Exception {
        NodeTypeRegistry.register(root, new ByteArrayInputStream(ASSET_NODE_TYPE.getBytes()), "test nodeType");
        createIndex("dam:Asset", false);
        root.commit();
        factory.indexFieldProvider = new IndexFieldProviderImpl();
        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();

        String log = runIndexingTest(IndexFieldProviderImpl.class, true);
        assertEquals("[" + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0" + "]", log);
    }

    @Test
    public void testIndexingDynamicBoost() throws Exception {
        createAssetsIndexAndProperties(false, false);

        String log = runIndexingTest(LuceneDocumentMaker.class, true);
        assertEquals(
                "[" + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0, " +
                        "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 30.0, " +
                        "confidence is not finite: jcr:content/metadata/predictedTags, " +
                        "confidence is not finite: jcr:content/metadata/predictedTags, " +
                        "confidence parsing failed: jcr:content/metadata/predictedTags, " +
                        "confidence parsing failed: jcr:content/metadata/predictedTags, " +
                        "confidence is an array: jcr:content/metadata/predictedTags, " +
                        "confidence is an array: jcr:content/metadata/predictedTags, " +
                        "name is an array: jcr:content/metadata/predictedTags, " +
                        "name is an array: jcr:content/metadata/predictedTags" +
                        "]", log);
    }

    @Test
    public void indexingDynamicBoostLite() throws Exception {
        createAssetsIndexAndProperties(true, false);

        String log = runIndexingTest(LuceneDocumentMaker.class, true);
        assertEquals("[]", log);
    }

    @Test
    public void indexingDynamicBoostLiteMissingProperty() throws Exception {
        createAssetsIndexAndProperties(true, false);

        String log = runIndexingTest(LuceneDocumentMaker.class, false);
        assertEquals("[]", log);
    }

    // dynamic boost: space is explained as OR instead of AND, this should be documented
    @Test
    public void queryWithSpace() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertEventually(() ->
                assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue flower')", SQL2,
                        List.of("/test/asset1", "/test/asset2", "/test/asset3")));

    }

    @Test
    public void dynamicBoostLiteCaseInsensitive() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'FLOWER')", SQL2, List.of("/test/asset1", "/test/asset2"));
    }

    @Test
    public void dynamicBoostLiteQueryWithSpace() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'coffee flower')", SQL2, List.of("/test/asset2"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue   plant')", SQL2, List.of("/test/asset3"));
    }

    @Test
    public void dynamicBoostLiteShouldGiveLessRelevanceToTags() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'long OR plant')",
                List.of("/test/asset1", "/test/asset2", "/test/asset3"));
        assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'short OR coffee')",
                List.of("/test/asset3", "/test/asset2"));
    }

    @Override
    protected void createAssetsIndexAndProperties(boolean lite, boolean similarityTags) throws Exception {
        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();
        super.createAssetsIndexAndProperties(lite, similarityTags);
    }

    private String runIndexingTest(Class<?> loggerClass, boolean nameProperty) throws CommitFailedException {
        LogCustomizer customLogs = LogCustomizer.forLogger(loggerClass).enable(Level.TRACE).create();
        customLogs.starting();
        try {
            Tree test = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");
            Tree node = createNodeWithType(test, "item", "dam:Asset", "");
            Tree predicted = createNodeWithType(
                    createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, JcrConstants.NT_UNSTRUCTURED, ""), "metadata",
                            JcrConstants.NT_UNSTRUCTURED, ""), "predictedTags", JcrConstants.NT_UNSTRUCTURED, "");
            Tree t = createNodeWithType(predicted, "a", JcrConstants.NT_UNSTRUCTURED, "");
            if (nameProperty) {
                t.setProperty("name", "my:a");
            }
            t.setProperty("confidence", 10.0);
            root.commit();

            // this is not detected because updateCount is not set
            t.setProperty("confidence", 20);
            root.commit();

            // this is not detected
            if (nameProperty) {
                t.removeProperty("confidence");
                t.getParent().setProperty("updateCount", 1);
                root.commit();
            }

            // now we change an indexed property:
            // this is detected in the dynamicBoost case
            t.getParent().setProperty("updateCount", 2);
            t.setProperty("confidence", 30);
            root.commit();

            // we try with an invalid value:
            t.getParent().setProperty("updateCount", 3);
            t.setProperty("confidence", Double.NEGATIVE_INFINITY);
            root.commit();

            // we try with another invalid value:
            t.getParent().setProperty("updateCount", 4);
            t.setProperty("confidence", "xz");
            root.commit();

            // we try with an array:
            t.getParent().setProperty("updateCount", 5);
            t.setProperty("confidence", List.of(), Type.STRINGS);
            root.commit();

            // we try with an array:
            if (nameProperty) {
                t.getParent().setProperty("updateCount", 6);
                t.setProperty("name", List.of(), Type.STRINGS);
                root.commit();
            }

            return customLogs.getLogs().toString();
        } finally {
            customLogs.finished();
        }
    }

    private static class SimpleIndexAugmentorFactory extends IndexAugmentorFactory {
        IndexFieldProvider indexFieldProvider = IndexFieldProvider.DEFAULT;
        FulltextQueryTermsProvider queryTermsProvider = FulltextQueryTermsProvider.DEFAULT;

        @Override
        public IndexFieldProvider getIndexFieldProvider(String nodeType) {
            return indexFieldProvider;
        }

        @Override
        public FulltextQueryTermsProvider getFulltextQueryTermsProvider(String nodeType) {
            return queryTermsProvider;
        }
    }

}
