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

package org.apache.jackrabbit.oak.plugins.index.lucene.dynamicBoost;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexAugmentorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import static org.junit.Assert.assertEquals;

// dynamic boost: the - should not work for dynamic boost per Fabrizio, todo: should be documented
// dynamic boost lite: not like dynamic boost, it doesn't respect confidence as order, todo: should be documented

/**
 * Tests the dynamic boost indexing and queries
 */
public class DynamicBoostTest extends AbstractQueryTest {
    public static final String ASSET_NODE_TYPE = "[dam:Asset]\n" + " - * (UNDEFINED) multiple\n" + " - * (UNDEFINED)\n" + " + * (nt:base) = oak:TestNode VERSION";

    private final SimpleIndexAugmentorFactory factory = new SimpleIndexAugmentorFactory();

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
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

    @Test
    public void testIndexingFieldProvider() throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
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
    public void testIndexingDynamicBoostLite() throws Exception {
        createAssetsIndexAndProperties(true, false);

        String log = runIndexingTest(LuceneDocumentMaker.class, true);
        assertEquals("[]", log);
    }

    @Test
    public void testIndexingDynamicBoostMissingProperty() throws Exception {
        createAssetsIndexAndProperties(true, false);

        String log = runIndexingTest(LuceneDocumentMaker.class, false);
        assertEquals("[]", log);
    }

    @Test
    public void testQueryDynamicBoostBasic() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertEquals(
                "[dam:Asset] as [a] /* lucene:test-index(/oak:index/test-index) :fulltext:plant " +
                        "((jcr:content/metadata/predictedTags/plant:1 jcr:content/metadata/predictedTags/plant:1)^1.0E-4) ft:(\"plant\")\n" +
                        "  where contains([a].[*], 'plant') */",
                explain("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH));

        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH,
                Arrays.asList("/test/asset1", "/test/asset2", "/test/asset3"));
        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'flower')]", XPATH, Arrays.asList("/test/asset1", "/test/asset2"));
    }

    @Test
    public void testQueryDynamicBoostCaseInsensitive() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'FLOWER')]", XPATH, Arrays.asList("/test/asset1", "/test/asset2"));
    }

    @Test
    public void testQueryDynamicBoostOrder() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'plant')",
                Arrays.asList("/test/asset2", "/test/asset3", "/test/asset1"));
    }

    // dynamic boost: space is explained as OR instead of AND, this should be documented
    @Test
    public void testQueryDynamicBoostSpace() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue flower')", SQL2,
                Arrays.asList("/test/asset1", "/test/asset2", "/test/asset3"));
    }

    @Test
    public void testQueryDynamicBoostLiteBasic() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertEquals(
                "[dam:Asset] as [a] /* lucene:test-index(/oak:index/test-index) :fulltext:plant simtags:plant ft:(\"plant\")\n  where contains([a].[*], 'plant') */",
                explain("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH));
        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH,
                Arrays.asList("/test/asset1", "/test/asset2", "/test/asset3"));
        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'flower')]", XPATH, Arrays.asList("/test/asset1", "/test/asset2"));
    }

    @Test
    public void testQueryDynamicBoostLiteCaseInsensitive() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'FLOWER')", SQL2, Arrays.asList("/test/asset1", "/test/asset2"));
    }

    @Test
    public void testQueryDynamicBoostLiteWildcard() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blu?')", SQL2, Arrays.asList("/test/asset3"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'bl?e')", SQL2, Arrays.asList("/test/asset3"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, '?lue')", SQL2, Arrays.asList("/test/asset3"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'coff*')", SQL2, Arrays.asList("/test/asset2"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'co*ee')", SQL2, Arrays.asList("/test/asset2"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, '*ffee')", SQL2, Arrays.asList("/test/asset2"));
    }

    @Test
    public void testQueryDynamicBoostLiteSpace() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'coffee flower')", SQL2, Arrays.asList("/test/asset2"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue   plant')", SQL2, Arrays.asList("/test/asset3"));
    }

    @Test
    public void testQueryDynamicBoostLiteExplicitOr() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue OR flower')", SQL2,
                Arrays.asList("/test/asset1", "/test/asset2", "/test/asset3"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue OR coffee')", SQL2,
                Arrays.asList("/test/asset2", "/test/asset3"));
    }

    @Test
    public void testQueryDynamicBoostLiteMinus() throws Exception {
        createAssetsIndexAndProperties(true, true);
        prepareTestAssets();

        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'plant -flower')", SQL2, Arrays.asList("/test/asset3"));
        assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'flower -coffee')", SQL2, Arrays.asList("/test/asset1"));
    }

    // Section 4. utils and assistant methods
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
            t.setProperty("confidence", new ArrayList<>(), Type.STRINGS);
            root.commit();

            // we try with an array:
            if (nameProperty) {
                t.getParent().setProperty("updateCount", 6);
                t.setProperty("name", new ArrayList<>(), Type.STRINGS);
                root.commit();
            }

            return customLogs.getLogs().toString();
        } finally {
            customLogs.finished();
        }
    }

    private void prepareTestAssets() throws CommitFailedException {
        Tree testParent = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        Tree predicted1 = createAssetNodeWithPredicted(testParent, "asset1", "titleone long");
        createPredictedTag(predicted1, "plant", 0.1);
        createPredictedTag(predicted1, "flower", 0.1);

        Tree predicted2 = createAssetNodeWithPredicted(testParent, "asset2", "titletwo long");
        createPredictedTag(predicted2, "plant", 0.9);
        createPredictedTag(predicted2, "flower", 0.1);
        createPredictedTag(predicted2, "coffee", 0.5);

        Tree predicted3 = createAssetNodeWithPredicted(testParent, "asset3", "titlethree short");
        createPredictedTag(predicted3, "plant", 0.5);
        createPredictedTag(predicted3, "blue", 0.5);
        root.commit();
    }

    private Tree createAssetNodeWithPredicted(Tree parent, String assetNodeName, String assetTitle) {
        Tree node = createNodeWithType(parent, assetNodeName, "dam:Asset", "");
        Tree predicted = createNodeWithType(
                createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, JcrConstants.NT_UNSTRUCTURED, assetTitle), "metadata",
                        JcrConstants.NT_UNSTRUCTURED, ""), "predictedTags", JcrConstants.NT_UNSTRUCTURED, "");
        return predicted;
    }

    private void createPredictedTag(Tree parent, String tagName, double confidence) {
        Tree node = createNodeWithType(parent, tagName, JcrConstants.NT_UNSTRUCTURED, "");
        node.setProperty("name", tagName);
        node.setProperty("confidence", confidence);
    }

    private Tree createNodeWithType(Tree parent, String nodeName, String nodeType, String title) {
        Tree node = parent.addChild(nodeName);
        node.setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
        if (StringUtils.isNotEmpty(title)) {
            node.setProperty("title", title, Type.STRING);
        }
        return node;
    }

    private void createAssetsIndexAndProperties(boolean lite, boolean similarityTags) throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        Tree props = createIndex("dam:Asset", lite);

        Tree predictedTagsDynamicBoost = createNodeWithType(props, "predictedTagsDynamicBoost", JcrConstants.NT_UNSTRUCTURED, "");
        predictedTagsDynamicBoost.setProperty("name", "jcr:content/metadata/predictedTags/.*");
        predictedTagsDynamicBoost.setProperty("isRegexp", true);
        predictedTagsDynamicBoost.setProperty("dynamicBoost", true);
        predictedTagsDynamicBoost.setProperty("propertyIndex", true);
        predictedTagsDynamicBoost.setProperty("nodeScopeIndex", true);

        Tree titleBoost = createNodeWithType(props, "title", JcrConstants.NT_UNSTRUCTURED, "");
        titleBoost.setProperty("name", "jcr:content/title");
        titleBoost.setProperty("propertyIndex", true);
        titleBoost.setProperty("nodeScopeIndex", true);

        if (similarityTags) {
            Tree predictedTags = createNodeWithType(props, "predictedTags", JcrConstants.NT_UNSTRUCTURED, "");
            predictedTags.setProperty("name", "jcr:content/metadata/predictedTags/*/name");
            predictedTags.setProperty("isRegexp", true);
            predictedTags.setProperty("similarityTags", true);
        }
        root.commit();

        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();
    }

    private Tree createIndex(String nodeType, boolean lite) throws Exception {
        Tree rootTree = root.getTree("/");
        Tree index = createTestIndexNode(rootTree, LuceneIndexConstants.TYPE_LUCENE);
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        index.setProperty(IndexConstants.DYNAMIC_BOOST_LITE_PROPERTY_NAME, lite);
        return TestUtil.newRulePropTree(index, nodeType);
    }

    // utils
    protected void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, AbstractQueryTest.SQL2, true, true);
        assertEquals(paths, result);
    }

    private String explain(String query, String language) {
        String explain = "explain " + query;
        return executeQuery(explain, language).get(0);
    }

    private InputStream toInputStream(String x) {
        return new ByteArrayInputStream(x.getBytes());
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
