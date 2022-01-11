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

/**
 * Tests the index augmentation feature.
 */
public class DynamicBoostTest extends AbstractQueryTest {

    public static final String ASSET_NODE_TYPE = "[dam:Asset]\n" + " - * (UNDEFINED) multiple\n" + " - * (UNDEFINED)\n" + " + * (nt:base) = oak:TestNode VERSION";

    private static final String UNSTRUCTURED = "nt:unstructured";

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
    public void withFieldProvider() throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        createIndex("dam:Asset");
        root.commit();
        factory.indexFieldProvider = new IndexFieldProviderImpl();
        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();

        String log = runIndexingTest(IndexFieldProviderImpl.class, true);
        assertEquals("[" + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0" + "]", log);
    }

    @Test
    public void withDynamicBoost() throws Exception {
        createDynamicBoostIndex(false);

        String log = runIndexingTest(LuceneDocumentMaker.class, true);
        assertEquals(
                "["
                        + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0, "
                        + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 30.0, "
                        + "confidence is not finite: jcr:content/metadata/predictedTags, "
                        + "confidence is not finite: jcr:content/metadata/predictedTags, "
                        + "confidence parsing failed: jcr:content/metadata/predictedTags, "
                        + "confidence parsing failed: jcr:content/metadata/predictedTags, "
                        + "confidence is an array: jcr:content/metadata/predictedTags, "
                        + "confidence is an array: jcr:content/metadata/predictedTags, "
                        + "name is an array: jcr:content/metadata/predictedTags, "
                        + "name is an array: jcr:content/metadata/predictedTags" + "]",
                log);
    }

    @Test
    public void withDynamicBoostLite() throws Exception {
        createDynamicBoostIndex(true);

        String log = runIndexingTest(LuceneDocumentMaker.class, true);
        assertEquals("[]", log);
    }

    @Test
    public void withDynamicBoostMissingProperty() throws Exception {
        createDynamicBoostIndex(true);

        String log = runIndexingTest(LuceneDocumentMaker.class, false);
        assertEquals("[]", log);
    }

    @Test
    public void testQueryDynamicBoost() throws Exception {
        createDynamicBoostIndex(false);

        factory.indexFieldProvider = new IndexFieldProviderImpl();
        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();

        Tree test = createNodeWithType(root.getTree("/"), "test", UNSTRUCTURED);

        Tree predicted1 = createAssetNode(test, "item1");
        createPredictedTag(predicted1, "red", 0.1);
        predicted1.setProperty("x", "red");

        Tree predicted2 = createAssetNode(test, "item2");
        createPredictedTag(predicted2, "red", 0.9);
        predicted2.setProperty("x", "red");

        Tree predicted3 = createAssetNode(test, "item3");
        createPredictedTag(predicted3, "red", 0.5);
        predicted3.setProperty("x", "red");

        root.commit();

        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'red')]", XPATH, Arrays.asList("/test/item1", "/test/item2", "/test/item3"));
        assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'red')", Arrays.asList("/test/item2", "/test/item3", "/test/item1"));
    }

    protected void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, AbstractQueryTest.SQL2, true, false);
        assertEquals(paths, result);
    }

    private Tree createAssetNode(Tree parent, String assetNodeName) throws CommitFailedException {
        Tree node = createNodeWithType(parent, assetNodeName, "dam:Asset");
        Tree predicted = createNodeWithType(
                createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, UNSTRUCTURED), "metadata", UNSTRUCTURED),
                "predictedTags", UNSTRUCTURED);
        return predicted;
    }

    private void createPredictedTag(Tree parent, String tagName, double confidence) {
        Tree t = createNodeWithType(parent, tagName, UNSTRUCTURED);
        t.setProperty("name", tagName);
        t.setProperty("confidence", confidence);
    }

    @Test
    public void testQueryDynamicBoostLite() throws Exception {
        createDynamicBoostIndex(true);

        factory.indexFieldProvider = new IndexFieldProviderImpl();
        factory.queryTermsProvider = new FulltextQueryTermsProviderImpl();

        Tree test = createNodeWithType(root.getTree("/"), "test", UNSTRUCTURED);
        Tree node = createNodeWithType(test, "item1", "dam:Asset");
        Tree predicted = createNodeWithType(
                createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, UNSTRUCTURED), "metadata", UNSTRUCTURED),
                "predictedTags", UNSTRUCTURED);

        predicted.setProperty("x", "red");

        Tree t = createNodeWithType(predicted, "red", UNSTRUCTURED);
        t.setProperty("name", "red");
        t.setProperty("confidence", 1.0);
        root.commit();

        assertQuery("//element(*, dam:Asset)[jcr:contains(., 'red')]", XPATH, Arrays.asList("/test/item1"));
    }

    private String runIndexingTest(Class<?> loggerClass, boolean nameProperty) throws CommitFailedException {
        LogCustomizer customLogs = LogCustomizer.forLogger(loggerClass).enable(Level.TRACE).create();
        customLogs.starting();
        try {
            Tree test = createNodeWithType(root.getTree("/"), "test", UNSTRUCTURED);
            Tree node = createNodeWithType(test, "item", "dam:Asset");
            Tree predicted = createNodeWithType(
                    createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, UNSTRUCTURED), "metadata", UNSTRUCTURED),
                    "predictedTags", UNSTRUCTURED);
            Tree t = createNodeWithType(predicted, "a", UNSTRUCTURED);
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

    private Tree createNodeWithType(Tree t, String nodeName, String typeName) {
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    private void createDynamicBoostIndex(boolean lite) throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        Tree props = createIndex("dam:Asset", lite);

        Tree predictedTagsDynamicBoost = createNodeWithType(props, "predictedTags", UNSTRUCTURED);
        predictedTagsDynamicBoost.setProperty("name", "jcr:content/metadata/predictedTags/.*");
        predictedTagsDynamicBoost.setProperty("isRegexp", true);
        predictedTagsDynamicBoost.setProperty("dynamicBoost", true);
        predictedTagsDynamicBoost.setProperty("propertyIndex", true);
        predictedTagsDynamicBoost.setProperty("nodeScopeIndex", true);

        root.commit();
    }

    private Tree createIndex(String nodeType) throws Exception {
        return createIndex(nodeType, false);
    }

    private Tree createIndex(String nodeType, boolean lite) throws Exception {
        Tree rootTree = root.getTree("/");
        Tree index = createTestIndexNode(rootTree, LuceneIndexConstants.TYPE_LUCENE);
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        index.setProperty(IndexConstants.DYNAMIC_BOOST_LITE_PROPERTY_NAME, lite);
        return TestUtil.newRulePropTree(index, nodeType);
    }

    private String explain(String query, String language) {
        String explain = "explain " + query;
        return executeQuery(explain, language).get(0);
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

    private static InputStream toInputStream(String x) {
        return new ByteArrayInputStream(x.getBytes());
    }
}
