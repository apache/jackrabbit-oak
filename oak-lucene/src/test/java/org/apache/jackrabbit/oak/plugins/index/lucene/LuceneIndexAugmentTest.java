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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.indexAugment.IndexAugmentorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.indexAugment.impl.IndexAugmentorFactoryImpl;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LuceneIndexAugmentTest extends AbstractQueryTest {
    private final SimpleIndexAugmentorFactory factory = new SimpleIndexAugmentorFactory();

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
                new ExtractedTextCache(0, 0),
                factory);
        LuceneIndexProvider provider = new LuceneIndexProvider(new IndexTracker(),
                ScorerProviderFactory.DEFAULT,
                factory);
        return new Oak()
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    //OAK-3789
    @Test
    public void skipDefaultIndexing() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);

        Tree prop = TestUtil.enablePropertyIndex(props, "subChild/foo", false);
        prop.setProperty(LuceneIndexConstants.PROP_SKIP_DEFAULT_INDEXING, true);

        prop = TestUtil.enablePropertyIndex(props, "foo1", false);
        prop.setProperty(LuceneIndexConstants.PROP_SKIP_DEFAULT_INDEXING, true);

        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item1", TestUtil.NT_TEST);
        node.setProperty("foo1", "bar1");
        Tree subChild = node.addChild("subChild");
        subChild.setProperty("foo", "bar");
        root.commit();

        //queries
        String query = "select * from [oak:TestNode] AS s where [subChild/foo]='bar'";
        List<String> paths = executeQuery(query, SQL2);
        assertEquals("There should not be any rows", 0, paths.size());

        query = "select * from [oak:TestNode] AS s where [foo1]='bar1'";
        paths = executeQuery(query, SQL2);
        assertEquals("There should not be any rows", 0, paths.size());
    }

    //OAK-3576
    @Test public void queryHook() throws Exception {
        //setup repo and index
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree props = createIndex(TestUtil.NT_TEST);
        TestUtil.enableForFullText(props, "foo");
        root.commit();

        //setup query augmentor
        final String searchText = "search this text";
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer) {
                assertEquals("Full text term passed to provider isn't same as the one passed in query",
                        searchText, text);
                return new TermQuery(new Term(":fulltext", "bar"));
            }
        };

        //add content
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item", TestUtil.NT_TEST);
        node.setProperty("foo", "bar");
        root.commit();

        //query (searchText doesn't have 'bar'... our augment would search for :fulltext:bar
        String query = "select [jcr:path] from [oak:TestNode] where CONTAINS(*, '" + searchText + "')";
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
            @Override
            public Iterable<Field> getAugmentedFields(String path, String propertyName,
                                                      NodeState document, PropertyState property,
                                                      NodeState indexDefinition) {
                counter.incrementAndGet();
                return null;
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
        assertEquals("Number of callbacks should be same as number of changed properties", 3, counter.get());

        //change sub-property
        counter.set(0);
        subChild = root.getTree("/test/item/subChild");
        subChild.setProperty("foo3", "bar4");
        root.commit();
        assertEquals("Sub child property change should make call backs for all indexed properties", 3, counter.get());
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
            @Override
            public Iterable<Field> getAugmentedFields(String path, String propertyName,
                                                      NodeState document, PropertyState property,
                                                      NodeState indexDefinition) {
                assertEquals("/test/item", path);
                assertEquals(propName, propertyName);
                assertEquals(TestUtil.NT_TEST, document.getName(JcrConstants.JCR_PRIMARYTYPE));
                assertEquals("foo", property.getName());
                assertEquals(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                        indexDefinition.getName(JcrConstants.JCR_PRIMARYTYPE));
                return Lists.<Field>newArrayList(new StringField(
                        property.getValue(Type.STRING) + property.getValue(Type.STRING),
                        "1",
                        Field.Store.NO
                ));
            }
        };
        factory.fulltextQueryTermsProvider = new FulltextQueryTermsProvider() {
            @Override
            public Query getQueryTerm(String text, Analyzer analyzer) {
                return new TermQuery(new Term(text, "1"));
            }
        };

        //add content
        Tree test = root.getTree("/").addChild("test");
        Tree node = createNodeWithType(test, "item", TestUtil.NT_TEST).addChild("subChild");
        node.setProperty("foo", "bar");
        root.commit();

        //do some queries... explicitly looking for terms we augmented (i.e. barbar=1)
        String query = "select [jcr:path] from [oak:TestNode] AS s where CONTAINS(*, 'barbar')";
        List<String> paths = executeQuery(query, SQL2);
        List<String> expected = Collections.singletonList("/test/item");
        assertThat(paths, is(expected));
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
        return TestUtil.newRulePropTree(index, nodeType);
    }

    private static class SimpleIndexAugmentorFactory implements IndexAugmentorFactory {
        private IndexFieldProvider indexFieldProvider = null;
        private FulltextQueryTermsProvider fulltextQueryTermsProvider = null;

        @Override
        public IndexFieldProvider getIndexFieldProvider() {
            return indexFieldProvider==null?
                    IndexAugmentorFactoryImpl.DEFAULT.getIndexFieldProvider():
                    indexFieldProvider;
        }

        @Override
        public FulltextQueryTermsProvider getFulltextQueryTermsProvider() {
            return fulltextQueryTermsProvider==null?
                    IndexAugmentorFactoryImpl.DEFAULT.getFulltextQueryTermsProvider():
                    fulltextQueryTermsProvider;
        }
    }
}
