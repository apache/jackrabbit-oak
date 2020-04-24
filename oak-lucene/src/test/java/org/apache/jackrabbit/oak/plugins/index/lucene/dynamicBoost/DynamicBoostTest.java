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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexAugmentorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
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

/**
 * Tests the index augmentation feature.
 */
public class DynamicBoostTest extends AbstractQueryTest {

    public static final String ASSET_NODE_TYPE =
            "[dam:Asset]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:TestNode VERSION";

    private static final String UNSTRUCTURED = "nt:unstructured";

    private final SimpleIndexAugmentorFactory factory = new SimpleIndexAugmentorFactory();

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        IndexTracker tracker = new IndexTracker();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
            new ExtractedTextCache(0, 0),
            factory, Mounts.defaultMountInfoProvider());
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker,
            factory);
        return new Oak()
            .with(new OpenSecurityProvider())
            .with((QueryIndexProvider) provider)
            .with((Observer) provider)
            .with(editorProvider)
            .createContentRepository();
    }

    @Test public void withFieldProvider() throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        createIndex("dam:Asset");
        root.commit();
        factory.indexFieldProvider = new IndexFieldProviderImpl();

        String log = runTest(IndexFieldProviderImpl.class);
        assertEquals(
                "[" +
                "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0" +
                "]", log);
    }

    @Test public void withDynamicBoost() throws Exception {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        Tree props = createIndex("dam:Asset");
        Tree pt = createNodeWithType(props, "predictedTags", UNSTRUCTURED);
        pt.setProperty("name", "jcr:content/metadata/predictedTags/.*");
        pt.setProperty("isRegexp", true);
        pt.setProperty("dynamicBoost", true);
        pt.setProperty("propertyIndex", true);
        root.commit();

        String log = runTest(LuceneDocumentMaker.class);
        assertEquals(
                "[" +
                "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0, " +
                "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 30.0" +
                "]", log);
    }

    private String runTest(Class<?> loggerClass) throws CommitFailedException {
        LogCustomizer customLogs = LogCustomizer
                .forLogger(loggerClass)
                .enable(Level.TRACE).create();
        customLogs.starting();
        try {
            Tree test = createNodeWithType(root.getTree("/"), "test", UNSTRUCTURED);
            Tree node = createNodeWithType(test, "item", "dam:Asset");
            Tree predicted =
                    createNodeWithType(
                    createNodeWithType(
                    createNodeWithType(node, JcrConstants.JCR_CONTENT, UNSTRUCTURED),
                    "metadata", UNSTRUCTURED),
                    "predictedTags", UNSTRUCTURED);
            Tree t = createNodeWithType(predicted, "a", UNSTRUCTURED);
            t.setProperty("name", "my:a");
            t.setProperty("confidence", 10.0);
            root.commit();
            // this is not detected
            t.setProperty("confidence", 20);
            root.commit();
            // now we change an indexed property:
            // this is detected in the dynamicBoost case
            t.getParent().setProperty("updateCount", 2);
            t.setProperty("confidence", 30);
            root.commit();
            return customLogs.getLogs().toString();
        } finally {
            customLogs.finished();
        }
    }

    private static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    private Tree createIndex(String nodeType) throws Exception {
        Tree rootTree = root.getTree("/");
        Tree index = createTestIndexNode(rootTree, LuceneIndexConstants.TYPE_LUCENE);
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        return TestUtil.newRulePropTree(index, nodeType);
    }

    private static class SimpleIndexAugmentorFactory extends IndexAugmentorFactory {
        IndexFieldProvider indexFieldProvider = IndexFieldProvider.DEFAULT;

        @Override
        public IndexFieldProvider getIndexFieldProvider(String nodeType) {
            return indexFieldProvider;
        }

    }

    private static InputStream toInputStream(String x) {
        return new ByteArrayInputStream(x.getBytes());
    }

}
