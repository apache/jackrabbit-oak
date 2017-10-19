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

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class LuceneIndexAugmentTest extends AbstractQueryTest {
    private final SimpleIndexAugmentorFactory factory = new SimpleIndexAugmentorFactory();

    private IndexTracker tracker = new IndexTracker();

    private IndexNode indexNode;

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
                new ExtractedTextCache(0, 0),
                factory, Mounts.defaultMountInfoProvider());
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker,
                ScorerProviderFactory.DEFAULT,
                factory);
        return new Oak()
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    //OAK-3576
    @Test public void queryHook() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo");
        root.commit();

        //setup query augmentor
        final String testSearchText = "search this text";
        final String realSearchText = "bar";
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                assertEquals("Full text term passed to provider isn't same as the one passed in query",
                        testSearchText, text);
                return new TermQuery(new Term(":fulltext", realSearchText));
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item", TestUtil.NT_TEST);
        node.setProperty("foo", realSearchText);
        root.commit();

        //query (testSearchText doesn't have 'bar'... our augment would search for :fulltext:bar
        String query = "select [jcr:path] from [oak:TestNode] where CONTAINS(*, '" + testSearchText + "')";
        List<String> paths = executeQuery(query, SQL2);
        assertEquals("Augmented query wasn't used to search", 1, paths.size());
        assertEquals("/test/item", paths.get(0));
    }

    //OAK-3576
    @Test public void indexHookCallbackFrequency() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enablePropertyIndex(props, "foo1", false);
        TestUtil.enablePropertyIndex(props, "foo2", false);
        TestUtil.enablePropertyIndex(props, "subChild/foo3", false);
        root.commit();

        //setup index augmentor
        final AtomicInteger counter = new AtomicInteger(0);
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                counter.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        counter.set(0);
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item", TestUtil.NT_TEST);
        node.setProperty("foo1", "bar1");
        node.setProperty("foo2", "bar2");
        Tree subChild = node.addChild("subChild");
        subChild.setProperty("foo3", "bar3");
        root.commit();
        assertEquals("Number of callbacks should be same as number of changed properties", 1, counter.get());

        //change sub-property
        counter.set(0);
        subChild = root.getTree("/test/item/subChild");
        subChild.setProperty("foo3", "bar4");
        root.commit();
        assertEquals("Sub child property change should make call backs for all indexed properties", 1, counter.get());
    }

    //OAK-3576
    @Test public void indexHookCallbackAndStorage() throws Exception {
        final String propName = "subChild/foo";

        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, propName);
        root.commit();

        //setup index augmentor
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                assertEquals("/test/item", path);
                assertEquals(TestUtil.NT_TEST, document.getName(JcrConstants.JCR_PRIMARYTYPE));
                assertEquals(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                        indexDefinition.getName(JcrConstants.JCR_PRIMARYTYPE));
                return Lists.<Field>newArrayList(new StringField("barbar", "1", Field.Store.NO));
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item", TestUtil.NT_TEST).addChild("subChild");
        node.setProperty("foo", "bar");
        root.commit();

        //Check document that made to the index
        IndexSearcher searcher = getSearcher();
        TopDocs docs = searcher.search(new TermQuery(new Term("barbar", "1")), 10);
        ScoreDoc[] scoreDocs = docs.scoreDocs;
        assertEquals("Number of results should be 1", 1, scoreDocs.length);
        Document doc = searcher.doc(scoreDocs[0].doc);
        String path = doc.get(":path");
        assertEquals("/test/item", path);
    }

    //OAK-3576
    @Test
    public void nullBehavior() throws Exception {
        // OAK-6833
        assumeFalse(CIHelper.windows());

        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo");
        root.commit();

        Tree rootTree = root.getTree("/").addChild("test");

        //Note: augmentor behavior is tested elsewhere... we are just checking if default works

        int testIndex = 1;
        //both query and index augmentors are null (no exception expected)
        checkSimpleBehavior(rootTree, testIndex++);

        //Set a very sad query augmentor
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return FulltextQueryTermsProvider.DEFAULT.getSupportedTypes();
            }
        };
        checkSimpleBehavior(rootTree, testIndex++);

        //Set query augmentor... with null query
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        checkSimpleBehavior(rootTree, testIndex++);

        //Set query augmentor... with some query
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                return new TermQuery(new Term("bar", "baz"));
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        checkSimpleBehavior(rootTree, testIndex++);

        factory.fulltextQueryTermsProvider = null;

        //Set a very sad index augmentor
        factory.indexFieldProvider = IndexFieldProvider.DEFAULT;
        checkSimpleBehavior(rootTree, testIndex++);

        //Set index augmentor... with null fields
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        checkSimpleBehavior(rootTree, testIndex++);

        //Set index augmentor... with some fields
        factory.fulltextQueryTermsProvider = null;
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                List<Field> fields = Lists.newArrayList();
                fields.add(new StringField("bar", "baz", Field.Store.NO));
                return fields;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        checkSimpleBehavior(rootTree, testIndex++);

        //setup composite query term provider with one returning null query
        factory.registerQueryTermsProvider(new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        });
        factory.useSuperBehavior = true;
        checkSimpleBehavior(rootTree, testIndex++);
    }

    //OAK-3576
    @Test
    public void skipDefaultOnlyUsingAugmentors() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        Tree prop = props.addChild("foo1");
        prop.setProperty(LuceneIndexConstants.PROP_INDEX, true);
        prop = props.addChild("foo2");
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "subChild/foo2");
        prop.setProperty(LuceneIndexConstants.PROP_INDEX, true);
        root.commit();

        //setup augmentors
        final AtomicInteger indexingCounter = new AtomicInteger(0);
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                indexingCounter.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        final AtomicInteger queryingCounter = new AtomicInteger(0);
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                queryingCounter.set(1);
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        Tree node1 = createNodeWithType(root.getTree("/"), "node1", TestUtil.NT_TEST);
        node1.setProperty("foo1", "bar1");
        node1.addChild("subChild").setProperty("foo2", "bar2");
        root.commit();

        //indexing assertions
        assertEquals("Indexing augment should get called once", 1, indexingCounter.get());
        assertEquals("No docs should get indexed (augmentor hasn't added any field)",
                0, getSearcher().getIndexReader().numDocs());

        String query = "EXPLAIN SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE [foo1]='bar1'";
        List<String> paths = executeQuery(query, SQL2);
        assertTrue("indexed prop name shouldn't decide query plan (" + paths.get(0) + ")",
                paths.get(0).contains("/* no-index "));

        query = "EXPLAIN SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE [subChild/foo2]='bar2'";
        paths = executeQuery(query, SQL2);
        assertTrue("indexed prop name shouldn't decide query plan (" + paths.get(0) + ")",
                paths.get(0).contains("/* no-index "));
    }

    //OAK-3576
    @Test
    public void propertyIndexUsingAugmentors() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enablePropertyIndex(props, "foo1", false);
        TestUtil.enablePropertyIndex(props, "subChild/foo2", false);
        root.commit();

        //setup augmentors
        final AtomicInteger indexingCounter = new AtomicInteger(0);
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                indexingCounter.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        final AtomicInteger queryingCounter = new AtomicInteger(0);
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                queryingCounter.set(1);
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        Tree node1 = createNodeWithType(root.getTree("/"), "node1", TestUtil.NT_TEST);
        node1.setProperty("foo1", "bar1");
        node1.addChild("subChild").setProperty("foo2", "bar2");
        root.commit();

        //indexing assertions
        assertEquals("Indexing augment should get called once", 1, indexingCounter.get());

        String query = "SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE [foo1]='bar1'";
        executeQuery(query, SQL2);
        assertEquals("Query augmentor should not get called for property constraints", 0, queryingCounter.get());
        query = "EXPLAIN " + query;
        List<String> paths = executeQuery(query, SQL2, false);
        assertTrue("property index should have made the index selected (" + paths.get(0) + ")",
                paths.get(0).contains("/* lucene:test-index("));

        query = "SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE [subChild/foo2]='bar2'";
        executeQuery(query, SQL2);
        assertEquals("Query augmentor should not get called for property constraints", 0, queryingCounter.get());
        query = "EXPLAIN " + query;
        paths = executeQuery(query, SQL2);
        assertTrue("property index should have made the index selected (" + paths.get(0) + ")",
                paths.get(0).contains("/* lucene:test-index("));
    }

    //OAK-3576
    @Test
    public void fulltextIndexUsingAugmentors() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo1");
        TestUtil.enableForFullText(props, "subChild/foo2");
        root.commit();

        //setup augmentors
        final AtomicInteger indexingCounter = new AtomicInteger(0);
        factory.indexFieldProvider = new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                indexingCounter.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };
        final AtomicInteger queryingCounter = new AtomicInteger(0);
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                queryingCounter.set(1);
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        };

        //add content
        Tree node1 = createNodeWithType(root.getTree("/"), "node1", TestUtil.NT_TEST);
        node1.setProperty("foo1", "bar1");
        node1.addChild("subChild").setProperty("foo2", "bar2");
        root.commit();

        //indexing assertions
        assertEquals("Indexing augment should get called once", 1, indexingCounter.get());

        String query = "SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE CONTAINS(*, 'bar1')";
        executeQuery(query, SQL2);
        assertEquals("Query augmentor should get called for full text constraints", 1, queryingCounter.get());
        queryingCounter.set(0);
        query = "EXPLAIN " + query;
        List<String> paths = executeQuery(query, SQL2, false);
        assertEquals("Query augmentor should get called for full text constraints", 1, queryingCounter.get());
        assertTrue("property index should have made the index selected (" + paths.get(0) + ")",
                paths.get(0).contains("/* lucene:test-index("));

        queryingCounter.set(0);
        query = "SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE CONTAINS(*, 'bar2')";
        executeQuery(query, SQL2);
        assertEquals("Query augmentor should get called for full text constraints", 1, queryingCounter.get());
        queryingCounter.set(0);
        query = "EXPLAIN " + query;
        paths = executeQuery(query, SQL2, false);
        assertEquals("Query augmentor should get called for full text constraints", 1, queryingCounter.get());
        assertTrue("property index should have made the index selected (" + paths.get(0) + ")",
                paths.get(0).contains("/* lucene:test-index("));
    }

    @Test
    public void indexAugmentorMismatchedNodeType() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo1");
        root.commit();

        //setup augmentors
        final AtomicInteger indexingCounter1 = new AtomicInteger(0);
        final AtomicInteger indexingCounter2 = new AtomicInteger(0);
        factory.registerIndexFieldProvider(new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                indexingCounter1.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(JcrConstants.NT_BASE);
            }
        });
        factory.registerIndexFieldProvider(new IndexFieldProvider() {
            @Nonnull
            @Override
            public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
                indexingCounter2.incrementAndGet();
                return IndexFieldProvider.DEFAULT.getAugmentedFields(path, document, indexDefinition);
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        });
        factory.useSuperBehavior = true;

        //add content
        createNodeWithType(root.getTree("/"), "node1", TestUtil.NT_TEST).setProperty("foo1", "bar1");
        root.commit();

        assertEquals("Mismatching node type should not let index augmentor called", 0, indexingCounter1.get());
        assertEquals("Matching node type should get augmentor called", 1, indexingCounter2.get());
    }

    @Test
    public void queryAugmentorMismatchedNodeType() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo1", false);
        root.commit();

        //setup augmentors
        final AtomicInteger indexingCounter1 = new AtomicInteger(0);
        final AtomicInteger indexingCounter2 = new AtomicInteger(0);
        factory.registerQueryTermsProvider(new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                indexingCounter1.set(1);
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(JcrConstants.NT_BASE);
            }
        });
        factory.registerQueryTermsProvider(new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
                indexingCounter2.set(1);
                return null;
            }

            @Nonnull
            @Override
            public Set<String> getSupportedTypes() {
                return Collections.singleton(TestUtil.NT_TEST);
            }
        });
        factory.useSuperBehavior = true;


        executeQuery("SELECT [jcr:path] FROM [" + TestUtil.NT_TEST + "] WHERE CONTAINS(*, 'test')", SQL2, false);

        assertEquals("Mismatching node type should not let index augmentor called", 0, indexingCounter1.get());
        assertEquals("Matching node type should get augmentor called", 1, indexingCounter2.get());
    }

    private static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    private Tree createIndex(String nodeType) throws Exception {
        Tree rootTree = root.getTree("/");
        return createIndex(rootTree, nodeType);
    }

    private Tree createIndex(Tree root, String nodeType) throws Exception {
        Tree index = createTestIndexNode(root, LuceneIndexConstants.TYPE_LUCENE);
        TestUtil.useV2(index);
        return TestUtil.newRulePropTree(index, nodeType);
    }

    private static class SimpleIndexAugmentorFactory extends IndexAugmentorFactory {
        IndexFieldProvider indexFieldProvider = null;
        FulltextQueryTermsProvider fulltextQueryTermsProvider = null;
        private boolean useSuperBehavior = false;

        void registerIndexFieldProvider(IndexFieldProvider provider) {
            bindIndexFieldProvider(provider);
        }

        void registerQueryTermsProvider(FulltextQueryTermsProvider provider) {
            bindFulltextQueryTermsProvider(provider);
        }

        @Nonnull
        @Override
        public IndexFieldProvider getIndexFieldProvider(String nodeType) {
            return useSuperBehavior?
                    super.getIndexFieldProvider(nodeType):
                    (indexFieldProvider != null)?
                            indexFieldProvider:
                            IndexFieldProvider.DEFAULT;
        }

        @Nonnull
        @Override
        public FulltextQueryTermsProvider getFulltextQueryTermsProvider(String nodeType) {
            return useSuperBehavior?
                    super.getFulltextQueryTermsProvider(nodeType):
                    (fulltextQueryTermsProvider != null)?
                        fulltextQueryTermsProvider:
                        FulltextQueryTermsProvider.DEFAULT;
        }
    }

    private IndexSearcher getSearcher(){
        if(indexNode == null){
            indexNode = tracker.acquireIndexNode("/oak:index/" + TEST_INDEX_NAME);
        }
        return indexNode.getSearcher();
    }

    private void checkSimpleBehavior(Tree rootTree, int testIndex) throws Exception {
        createNodeWithType(rootTree, "node" + testIndex, TestUtil.NT_TEST)
                .setProperty("foo", "bar" + testIndex);
        root.commit();

        String query = "SELECT [jcr:path] from [" + TestUtil.NT_TEST + "] WHERE contains(*, 'bar" + testIndex + "')";
        List<String> paths = executeQuery(query, SQL2);
        assertEquals(1, paths.size());
        assertEquals("/test/node" + testIndex, paths.get(0));
    }
}
