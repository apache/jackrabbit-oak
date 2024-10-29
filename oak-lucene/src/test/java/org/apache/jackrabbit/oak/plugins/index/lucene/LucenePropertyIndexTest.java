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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.guava.common.collect.ComparisonChain;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.io.CountingInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.fv.SimSearchUtils;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.QUERY_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TIKA;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newNodeAggregator;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorTest.createCal;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_RANDOM_SEED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class LucenePropertyIndexTest extends AbstractQueryTest {
    /**
     * Set the size to twice the batch size to test the pagination with sorting
     */
    static final int NUMBER_OF_NODES = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2;

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String corDir = null;
    private String cowDir = null;

    private LuceneIndexEditorProvider editorProvider;

    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();

    private NodeStore nodeStore;

    private LuceneIndexProvider provider;

    private ResultCountingIndexProvider queryIndexProvider;

    private QueryEngineSettings queryEngineSettings = new QueryEngineSettings();

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier = createIndexCopier();
        editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10* FileUtils.ONE_MB, 100));
        provider = new LuceneIndexProvider(copier);
        queryIndexProvider = new ResultCountingIndexProvider(provider);
        nodeStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(queryIndexProvider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(queryEngineSettings)
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot()) {
                @Override
                public Directory wrapForRead(String indexPath, LuceneIndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(LuceneIndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName,
                                              COWDirectoryTracker cowDirectoryTracker) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName, cowDirectoryTracker);
                    cowDir = getFSDirPath(ret);
                    return ret;
                }

                private String getFSDirPath(Directory dir){
                    if (dir instanceof CopyOnReadDirectory){
                        dir = ((CopyOnReadDirectory) dir).getLocal();
                    }

                    dir = unwrap(dir);

                    if (dir instanceof FSDirectory){
                        return ((FSDirectory) dir).getDirectory().getAbsolutePath();
                    }
                    return null;
                }

                private Directory unwrap(Directory dir){
                    if (dir instanceof FilterDirectory){
                        return unwrap(((FilterDirectory) dir).getDelegate());
                    }
                    return dir;
                }

            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void shutdownExecutor(){
        executorService.shutdown();
    }

    private Tree createFulltextIndex(Tree index, String name) throws CommitFailedException {
        return TestUtil.createFulltextIndex(index, name);
    }

    @Test
    public void indexSelection() throws Exception {
        createIndex("test1", Set.of("propa", "propb"));
        createIndex("test2", Set.of("propc"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
        assertThat(explain(propaQuery), containsString("lucene:test1"));
        assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"), containsString("lucene:test2"));

        assertQuery(propaQuery, asList("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", asList("/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", asList("/test/d"));
    }

    @Test
    public void indexSelectionVsNodeType() throws Exception {
        Tree luceneIndex = createIndex("test1", Set.of("propa"));
        // decrease cost of lucene property index
        luceneIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 5L, Type.LONG);

        // Decrease cost of node type index
        Tree nodeTypeIndex = root.getTree("/").getChild("oak:index").getChild("nodetype");
        nodeTypeIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 50L, Type.LONG);
        nodeTypeIndex.setProperty(IndexConstants.KEY_COUNT_PROPERTY_NAME, 10L, Type.LONG);

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = new ArrayList<>();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n"+idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("propa", "foo");
            paths.add("/test/n" + idx);
        }
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:unstructured] where [propa] = 'foo'";
        assertThat(explain(propaQuery), containsString("lucene:test1"));

        assertQuery(propaQuery, paths);
    }

    @Test
    public void indexSelectionFulltextVsNodeType() throws Exception {
        Tree nodeTypeIdx = root.getTree("/oak:index/nodetype");
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES));
        nodeTypeIdx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        //Set the cost to highest to ensure that if Lucene index opts in then
        //it always wins. In actual case Lucene index should not participate
        //in such queries
        nodeTypeIdx.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, Long.MAX_VALUE);

        Tree luceneIndex = createFullTextIndex(root.getTree("/"), "lucene");

        Tree test = root.getTree("/").addChild("test");
        setNodeType(test, "nt:file");

        setNodeType(test.addChild("a"), "nt:file");
        setNodeType(test.addChild("b"), "nt:file");
        setNodeType(test.addChild("c"), "nt:base");
        root.commit();

        String propabQuery = "/jcr:root//element(*, nt:file)";
        assertThat(explainXpath(propabQuery), containsString("nodeType"));
    }

    @Test
    public void declaringNodeTypeSameProp() throws Exception {
        createIndex("test1", Set.of("propa"));

        Tree indexWithType = createIndex("test2", Set.of("propa"));
        indexWithType.setProperty(PropertyStates
                .createProperty(DECLARING_NODE_TYPES, Set.of("nt:unstructured"),
                        Type.STRINGS));

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        Tree a = test.addChild("a");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("propa", "foo");
        Tree b = test.addChild("b");
        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("propa", "foo");

        test.addChild("c").setProperty("propa", "foo");
        test.addChild("d").setProperty("propa", "foo");

        root.commit();

        String propabQuery = "select [jcr:path] from [nt:unstructured] where [propa] = 'foo'";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b"));

        String propcdQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
        assertThat(explain(propcdQuery), containsString("lucene:test1"));
        assertQuery(propcdQuery, asList("/test/a", "/test/b", "/test/c", "/test/d"));
    }

    @Test
    public void declaringNodeTypeSingleIndex() throws Exception {
        Tree indexWithType = createIndex("test2", Set.of("propa", "propb"));
        indexWithType.setProperty(PropertyStates
            .createProperty(DECLARING_NODE_TYPES, Set.of("nt:unstructured"),
                Type.STRINGS));

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        Tree a = test.addChild("a");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("propa", "foo");
        a.setProperty("propb", "baz");

        Tree b = test.addChild("b");
        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("propa", "foo");
        b.setProperty("propb", "baz");

        root.commit();

        String propabQuery = "select [jcr:path] from [nt:unstructured] where [propb] = 'baz' and " +
            "[propa] = 'foo'";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b"));

        String propNoIdxQuery = "select [jcr:path] from [nt:base] where [propb] = 'baz'";
        assertThat(explain(propNoIdxQuery), containsString("no-index"));
        assertQuery(propNoIdxQuery, ImmutableList.of());
    }

    @Test
    public void usedAsNodeTypeIndex() throws Exception {
        Tree nodeTypeIdx = root.getTree("/oak:index/nodetype");
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, Set.of("nt:resource"), NAMES));
        nodeTypeIdx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        Tree indexWithType = createIndex("test2", Set.of(JcrConstants.JCR_PRIMARYTYPE, "propb"));
        indexWithType.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES));

        Tree test = root.getTree("/").addChild("test");
        setNodeType(test, "nt:file");
        root.commit();

        setNodeType(test.addChild("a"), "nt:file");
        setNodeType(test.addChild("b"), "nt:file");
        setNodeType(test.addChild("c"), "nt:base");
        root.commit();

        String propabQuery = "select [jcr:path] from [nt:file]";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b", "/test"));
    }

    @Test
    public void usedAsNodeTypeIndex2() throws Exception {
        //prevent the default nodeType index from indexing all types
        Tree nodeTypeIdx = root.getTree("/oak:index/nodetype");
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, Set.of("nt:resource"), NAMES));
        nodeTypeIdx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        Tree indexWithType = createIndex("test2", Set.of("propb"));
        indexWithType.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES));
        indexWithType.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, true);
        TestUtil.useV2(indexWithType);

        Tree test = root.getTree("/").addChild("test");
        setNodeType(test, "nt:file");
        root.commit();

        setNodeType(test.addChild("a"), "nt:file");
        setNodeType(test.addChild("b"), "nt:file");
        setNodeType(test.addChild("c"), "nt:base");
        root.commit();

        String propabQuery = "select [jcr:path] from [nt:file]";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b", "/test"));
    }

    private static Tree setNodeType(Tree t, String typeName){
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    @Test
    public void nodeName() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree rules = idx.addChild(FulltextIndexConstants.INDEX_RULES);
        rules.setOrderableChildren(true);
        Tree rule = rules.addChild("nt:base");
        rule.setProperty(LuceneIndexConstants.INDEX_NODE_NAME, true);
        root.commit();

        Tree test = root.getTree("/");
        test.addChild("foo");
        test.addChild("camelCase");
        test.addChild("test").addChild("bar");
        root.commit();

        String propabQuery = "select [jcr:path] from [nt:base] where LOCALNAME() = 'foo'";
        assertThat(explain(propabQuery), containsString(":nodeName:foo"));
        assertQuery(propabQuery, asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() = 'bar'", asList("/test/bar"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'foo'", asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'camel%'", asList("/camelCase"));

        assertQuery("select [jcr:path] from [nt:base] where NAME() = 'bar'", asList("/test/bar"));
        assertQuery("select [jcr:path] from [nt:base] where NAME() LIKE 'foo'", asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where NAME() LIKE 'camel%'", asList("/camelCase"));
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception{
        //make index
        Tree idx = createIndex("test1", Collections.EMPTY_SET);
        useV2(idx);
        Tree rules = idx.addChild(FulltextIndexConstants.INDEX_RULES);
        rules.setOrderableChildren(true);
        Tree rule = rules.addChild("nt:base");
        Tree propDef = rule.addChild(PROP_NODE).addChild("nodeName");
        propDef.setProperty(PROP_NAME, PROPDEF_PROP_NODE_NAME);
        propDef.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        //add content
        Tree test = root.getTree("/");
        test.addChild("foo");
        test.addChild("camelCase");
        test.addChild("test").addChild("bar");
        root.commit();

        //test
        String propabQuery = "select [jcr:path] from [nt:base] where LOCALNAME() = 'foo'";
        assertThat(explain(propabQuery), containsString(":nodeName:foo"));
        assertQuery(propabQuery, asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() = 'bar'", asList("/test/bar"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'foo'", asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'camel%'", asList("/camelCase"));

        assertQuery("select [jcr:path] from [nt:base] where NAME() = 'bar'", asList("/test/bar"));
        assertQuery("select [jcr:path] from [nt:base] where NAME() LIKE 'foo'", asList("/foo"));
        assertQuery("select [jcr:path] from [nt:base] where NAME() LIKE 'camel%'", asList("/camelCase"));
    }

    @Test
    public void emptyIndex() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"), containsString("lucene:test1"));
    }

    @Test
    public void explainScoreTest() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        String query = "select [oak:scoreExplanation] from [nt:base] where propa='a'";
        List<String> result = executeQuery(query, SQL2, false, false);
        assertEquals(1, result.size());
        assertTrue(result.get(0).contains("(MATCH)"));
    }

    //OAK-2568
    @Test
    public void multiValueAnd() throws Exception{
        Tree idx = createIndex("test1", Set.of("tags"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("tags", Set.of("a", "b"), Type.STRINGS);
        test.addChild("b").setProperty("tags", Set.of("a","c"), Type.STRINGS);
        root.commit();

        String q = "SELECT * FROM [nt:unstructured] as content WHERE ISDESCENDANTNODE('/content/dam/en/us')\n" +
                "and(\n" +
                "    content.[tags] = 'Products:A'\n" +
                "    or content.[tags] = 'Products:A/B'\n" +
                "    or content.[tags] = 'Products:A/B'\n" +
                "    or content.[tags] = 'Products:A'\n" +
                ")\n" +
                "and(\n" +
                "    content.[tags] = 'DocTypes:A'\n" +
                "    or content.[tags] = 'DocTypes:B'\n" +
                "    or content.[tags] = 'DocTypes:C'\n" +
                "    or content.[tags] = 'ProblemType:A'\n" +
                ")\n" +
                "and(\n" +
                "    content.[hasRendition] IS NULL\n" +
                "    or content.[hasRendition] = 'false'\n" +
                ")";
        String explain = explain(q);
        assertThat(explain, containsString("+(tags:Products:A tags:Products:A/B) " +
                "+(tags:DocTypes:A tags:DocTypes:B tags:DocTypes:C tags:ProblemType:A)"));
    }

    @Test
    public void multiValuesLike() throws Exception{
        Tree idx = createIndex("test1", Set.of("references"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("references", Set.of("/some/content/AAA", "/some/content/BBB"), Type.STRINGS);
        test.addChild("b").setProperty("references", Set.of("/some/content/AAA", "/some/content/CCC"), Type.STRINGS);
        root.commit();

        String q = "SELECT * FROM [nt:unstructured] as content WHERE references LIKE '/some/content/efjoiefjowfgj/%'";
        String explain = explain(q);
        assertThat(explain, containsString("references:/some/content/efjoiefjowfgj/*"));
    }

    //OAK-9481
    @Test
    public void likeQueryOnPropertiesWithExcludedPrefixes() throws Exception {
        Tree idx = createIndex("test1", Set.of("references"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("references");
        propIdx.setProperty("valueExcludedPrefixes", Set.of("/a/b/c2"), STRINGS);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("references", Set.of("/a/b/c1", "/a/b/c2", "/a/b/c10"), Type.STRINGS);
        test.addChild("b").setProperty("references", Set.of("/a/b/c22","/a/b/d12"), Type.STRINGS);
        root.commit();

        // index test1 not picked up because property restriction matches value excluded prefix.
        assertThat(explain("SELECT * FROM [nt:unstructured] as [content] WHERE [content].[references] LIKE '/a/b/c%'"),
                containsString("[nt:unstructured] as [content] /* nodeType"));
        // index test1 gets picked up because property restriction does not match value excluded prefix.
        assertThat(explain("SELECT * FROM [nt:unstructured] as [content] WHERE [content].[references] LIKE '/a/b/d%'"),
                containsString("luceneQuery: references:/a/b/d*"));

        assertQuery("SELECT [jcr:path] FROM [nt:base] as [content] WHERE [content].[references] LIKE '/a/b/c%'", SQL2, asList());
        assertQuery("SELECT [jcr:path] FROM [nt:base] WHERE references LIKE '/a/b/d%'", asList("/test/b"));
    }

    @Test
    public void redundantNotNullCheck() throws Exception{
        Tree idx = createIndex("test1", Set.of("tags"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("tags", Set.of("a","b"), Type.STRINGS);
        test.addChild("b").setProperty("tags", Set.of("a", "c"), Type.STRINGS);
        root.commit();

        String q = "SELECT * FROM [nt:unstructured] as content WHERE ISDESCENDANTNODE('/content/dam/en/us')\n" +
                "and(\n" +
                "    content.[tags] = 'Products:A'\n" +
                "    or content.[tags] = 'Products:A/B'\n" +
                "    or content.[tags] = 'Products:A/B'\n" +
                "    or content.[tags] = 'Products:A'\n" +
                ")\n" +
                "and(\n" +
                "    content.[tags] = 'DocTypes:A'\n" +
                "    or content.[tags] = 'DocTypes:B'\n" +
                "    or content.[tags] = 'DocTypes:C'\n" +
                "    or content.[tags] = 'ProblemType:A'\n" +
                ")\n" +
                "and(\n" +
                "    content.[hasRendition] IS NULL\n" +
                "    or content.[hasRendition] = 'false'\n" +
                ")";

        //Check that filter created out of query does not have is not null restriction
        assertThat(explain(q), not(containsString("[content].[tags] is not null")));
    }

    private static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    @Test
    public void rangeQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("b").setProperty("propa", 20);
        test.addChild("c").setProperty("propa", 30);
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] >= 20"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <= 20", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < 20", asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20 or [propa] = 10 ", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > 10 and [propa] < 30", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] in (10,20)", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void pathInclude() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, Set.of("/test/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10", asList("/test/a", "/test/a/b"));
    }

    //OAK-4517
    @Test
    public void pathIncludeSubrootIndex() throws Exception {
        Tree subTreeRoot = root.getTree("/").addChild("test");
        Tree idx = createIndex(subTreeRoot, "test1", Set.of("propa"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, Set.of("/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        subTreeRoot.addChild("a").setProperty("propa", 10);
        subTreeRoot.addChild("a").addChild("b").setProperty("propa", 10);
        subTreeRoot.addChild("c").setProperty("propa", 10);
        root.commit();

        String query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test')";
        assertThat(explain(query), containsString("lucene:test1"));
        assertQuery(query, asList("/test/a", "/test/a/b"));
    }

    //OAK-4517
    @Test
    public void pathQuerySubrootIndex() throws Exception {
        Tree subTreeRoot = root.getTree("/").addChild("test");
        Tree idx = createIndex(subTreeRoot, "test1", Set.of("propa"));
        idx.setProperty(createProperty(QUERY_PATHS, Set.of("/test/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        subTreeRoot.addChild("a").setProperty("propa", 10);
        subTreeRoot.addChild("a").addChild("b").setProperty("propa", 10);
        subTreeRoot.addChild("a").addChild("b").addChild("c").setProperty("propa", 10);
        subTreeRoot.addChild("c").setProperty("propa", 10);
        root.commit();

        String query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test/a')";
        String explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/test/a/b", "/test/a/b/c"));

        query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test/a/b')";
        explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/test/a/b/c"));

        query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test')";
        explanation = explain(query);
        assertThat(explanation, not(containsString("lucene:test1")));
        assertThat(explanation, containsString("/* no-index"));

        query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test/c')";
        explanation = explain(query);
        assertThat(explanation, not(containsString("lucene:test1")));
        assertThat(explanation, containsString("/* no-index"));
    }

    //OAK-4517
    @Test
    public void pathExcludeSubrootIndex() throws Exception{
        Tree subTreeRoot = root.getTree("/").addChild("test");
        Tree idx = createIndex(subTreeRoot, "test1", Set.of("propa"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, Set.of("/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        subTreeRoot.addChild("a").setProperty("propa", 10);
        subTreeRoot.addChild("a").addChild("b").setProperty("propa", 10);
        subTreeRoot.addChild("c").setProperty("propa", 10);
        root.commit();

        String query = "select [jcr:path] from [nt:base] where [propa] = 10 AND ISDESCENDANTNODE('/test')";

        assertThat(explain(query), containsString("lucene:test1"));
        assertQuery(query, asList("/test/c"));

        //Make some change and then check
        subTreeRoot = root.getTree("/").getChild("test");
        subTreeRoot.addChild("a").addChild("e").setProperty("propa", 10);
        subTreeRoot.addChild("f").setProperty("propa", 10);
        root.commit();

        assertQuery(query, asList("/test/c", "/test/f"));
    }

    @Test
    public void determinePropTypeFromRestriction() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        //Do not provide type information
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("b").setProperty("propa", 20);
        test.addChild("c").setProperty("propa", 30);
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] >= 20"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <= 20", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < 20", asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20 or [propa] = 10 ", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > 10 and [propa] < 30", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] in (10,20)", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));

    }

    @Test
    public void rangeQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10.1);
        test.addChild("b").setProperty("propa", 20.4);
        test.addChild("c").setProperty("propa", 30.7);
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20.3", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20.4", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <= 20.5", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < 20.4", asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > 10.5 and [propa] < 30", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void rangeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "b is b");
        test.addChild("c").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        test.addChild("d").setProperty("propb", "f");
        test.addChild("e").setProperty("propb", "g");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa = 'a'", asList("/test/a"));
        //Check that string props are not tokenized
        assertQuery("select [jcr:path] from [nt:base] where propa = 'b is b'", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa in ('a', 'c')", asList("/test/a", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] >= 'f'", asList("/test/d", "/test/e"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] <= 'f'", asList("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] > 'e'", asList("/test/d", "/test/e"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] < 'g'", asList("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void rangeQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", createCal("14/02/2014"));
        test.addChild("b").setProperty("propa", createCal("14/03/2014"));
        test.addChild("c").setProperty("propa", createCal("14/04/2014"));
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= " + dt("15/02/2014"), asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <=" + dt("15/03/2014"), asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < " + dt("14/03/2014"), asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > "+ dt("15/02/2014") + " and [propa] < " + dt("13/04/2014"), asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void nativeQueries() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        idx.setProperty(FulltextIndexConstants.FUNC_NAME, "foo");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "humpty");
        test.addChild("b").setProperty("propa", "dumpty");
        test.addChild("c").setProperty("propa", "humpy");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where native('foo', 'propa:(humpty OR dumpty)')",
                asList("/test/a", "/test/b"));
    }

    @Test
    public void testWithRelativeProperty() throws Exception{
        Tree parent = root.getTree("/");
        Tree idx = createIndex(parent, "test1", Set.of("b/propa", "propb"));
        root.commit();

        Tree test = parent.addChild("test2");
        test.addChild("a").addChild("b").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] as s where [b/propa] = 'a'", asList("/test2/a"));

    }

    @Test
    public void indexDefinitionBelowRoot() throws Exception {
        Tree parent = root.getTree("/").addChild("test");
        Tree idx = createIndex(parent, "test1", Set.of("propa", "propb"));
        idx.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = parent.addChild("test2");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] as s where ISDESCENDANTNODE(s, '/test') and propa = 'a'", asList("/test/test2/a"));
    }

    @Test
    public void indexDefinitionBelowRoot2() throws Exception {
        Tree parent = root.getTree("/").addChild("test");
        Tree idx = createIndex(parent, "test1", Set.of("propa", "propb"));
        idx.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = parent.addChild("test2").addChild("test3");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] as s where ISDESCENDANTNODE(s, '/test/test2') and propa = 'a'",
                asList("/test/test2/test3/a"));
    }

    @Test
    public void indexDefinitionBelowRoot3() throws Exception {
        Tree parent = root.getTree("/").addChild("test");
        Tree idx = createIndex(parent, "test1", Set.of("propa"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        parent.setProperty("propa", "a");
        parent.addChild("test1").setProperty("propa", "a");
        root.commit();

        //asert that (1) result gets returned correctly, (2) parent isn't there, and (3) child is returned
        assertQuery("select [jcr:path] from [nt:base] as s where ISDESCENDANTNODE(s, '/test') and propa = 'a'", asList("/test/test1"));
    }

    @Test
    public void queryPathRescrictionWithoutEvaluatePathRestriction() throws Exception {
        Tree parent = root.getTree("/");
        Tree idx = createIndex(parent, "test1", Set.of());
        idx.addChild(PROP_NODE).addChild("propa");

        Tree test = parent.addChild("test");
        test.addChild("a").addChild("c").addChild("d").setProperty("propa", "a");
        test.addChild("b").addChild("c").addChild("d").setProperty("propa", "a");

        parent = root.getTree("/").addChild("subRoot");
        idx = createIndex(parent, "test1", Set.of());
        idx.addChild(PROP_NODE).addChild("propa");

        test = parent.addChild("test");
        test.addChild("a").addChild("c").addChild("d").setProperty("propa", "a");
        test.addChild("b").addChild("c").addChild("d").setProperty("propa", "a");
        root.commit();

        queryIndexProvider.setShouldCount(true);

        // Top level index
        assertQuery("/jcr:root/test/a/c/d[@propa='a']", XPATH, asList("/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();

        assertQuery("/jcr:root/test/a/c/*[@propa='a']", XPATH, asList("/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();

        assertQuery("/jcr:root/test/a//*[@propa='a']", XPATH, asList("/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();

        // Sub-root index
        assertQuery("/jcr:root/subRoot/test/a/c/d[@propa='a']", XPATH, asList("/subRoot/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();

        assertQuery("/jcr:root/subRoot/test/a/c/*[@propa='a']", XPATH, asList("/subRoot/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();

        assertQuery("/jcr:root/subRoot/test/a//*[@propa='a']", XPATH, asList("/subRoot/test/a/c/d"));
        assertEquals("Unexpected number of docs passed back to query engine", 1, queryIndexProvider.getCount());
        queryIndexProvider.reset();
    }

    @Test
    public void sortQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_NotIndexed() throws Exception {
        Tree idx = createIndex("test1", Collections.<String>emptySet());
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] order by [jcr:score], [foo]"), containsString("lucene:test1"));

        assertThat(explain("select [jcr:path] from [nt:base] order by [foo]"), containsString("lucene:test1"));

        List<Tuple> tuples = createDataForLongProp();
        assertOrderedQuery("select [jcr:path] from [nt:base] order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base]  order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }


    @Test
    public void sortQueriesWithLong_NotIndexed_relativeProps() throws Exception {
        Tree idx = createIndex("test1", Collections.<String>emptySet());
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo/bar"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo").addChild("bar");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] order by [foo/bar]"), containsString("lucene:test1"));

        Tree test = root.getTree("/").addChild("test");
        List<Long> values = createLongs(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.addChild("foo").setProperty("bar", values.get(i));
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] order by [foo/bar]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base]  order by [foo/bar] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    void assertSortedLong() throws CommitFailedException {
        List<Tuple> tuples = createDataForLongProp();
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    private List<Tuple> createDataForLongProp() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Long> values = createLongs(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();
        return tuples;
    }

    @Test
    public void sortQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    @Test
    public void sortQueriesWithDouble_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    void assertSortedDouble() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Double> values = createDoubles(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
                "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
                getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    @Test
    public void sortQueriesWithString_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    @Test
    public void sortQueriesWithStringIgnoredMulti_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        List<String> values = createStrings(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n" + i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }

        //Add a wrong multi-valued property
        Tree child = test.addChild("a");
        child.setProperty("foo", Set.of("w", "z"), Type.STRINGS);
        child.setProperty("bar", "baz");
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", Lists
            .newArrayList(Iterables.concat(Lists.newArrayList("/test/a"), getSortedPaths(tuples, OrderDirection.ASC))));
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", Lists
            .newArrayList(Iterables.concat(getSortedPaths(tuples, OrderDirection.DESC), Lists.newArrayList("/test/a")
            )));
    }

    void assertSortedString() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<String> values = createStrings(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
                "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
                getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    @Test
    public void sortQueriesWithDate_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    void assertSortedDate() throws ParseException, CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]",
                getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
                "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
                getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDateStringMixed_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, Set.of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = new ArrayList<>(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("bar", "baz");
            if (i != 0) {
                child.setProperty("foo", values.get(i));
                tuples.add(new Tuple(values.get(i), child.getPath()));
            } else {
                child.setProperty("foo", String.valueOf(values.get(i).getTimeInMillis()));
            }
        }
        root.commit();

        // Add the path of property added as timestamp string in the sorted list
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]",
                CollectionUtils.toList(Iterables.concat(Lists.newArrayList("/test/n0"),
                        getSortedPaths(tuples, OrderDirection.ASC))));
        // Append the path of property added as timestamp string to the sorted list
        assertOrderedQuery(
                "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", Lists
                        .newArrayList(Iterables.concat(getSortedPaths(tuples, OrderDirection.DESC),
                                Lists.newArrayList("/test/n0"))));
    }

    @Test
    public void sortQueriesWithStringAndLong() throws Exception {
        Tree idx = createIndex("test1", Set.of("foo", "bar", "baz"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo", "baz"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("baz");
        propIdx.setProperty(PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        int firstPropSize = 5;
        List<String> values = createStrings(firstPropSize);
        List<Long> longValues = createLongs(NUMBER_OF_NODES);
        List<Tuple2> tuples = new ArrayList<>(values.size());
        Random r = new Random();
        for(int i = 0; i < values.size(); i++){
            String val = values.get(r.nextInt(firstPropSize));
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", val);
            child.setProperty("baz", longValues.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple2(val, longValues.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] asc, [baz] desc", getSortedPaths(tuples));
    }

    @Test
    public void indexTimeFieldBoost() throws Exception {
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa", "propb", "propc"));
        idx.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, true);

        Tree propNode = idx.addChild(PROP_NODE);

        // property definition for index test1
        Tree propA = propNode.addChild("propa");
        propA.setProperty(PROP_TYPE, PropertyType.TYPENAME_STRING);
        propA.setProperty(FulltextIndexConstants.FIELD_BOOST, 2.0);

        Tree propB = propNode.addChild("propb");
        propB.setProperty(PROP_TYPE, PropertyType.TYPENAME_STRING);
        propB.setProperty(FulltextIndexConstants.FIELD_BOOST, 1.0);

        Tree propC = propNode.addChild("propc");
        propC.setProperty(PROP_TYPE, PropertyType.TYPENAME_STRING);
        propC.setProperty(FulltextIndexConstants.FIELD_BOOST, 4.0);
        root.commit();

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propb", "foo");
        test.addChild("c").setProperty("propc", "foo");
        root.commit();

        String queryString = "//* [jcr:contains(., 'foo' )]";
        // verify results ordering
        // which should be /test/c (boost = 4.0), /test/a(boost = 2.0), /test/b (1.0)
        assertOrderedQuery(queryString, asList("/test/c", "/test/a", "/test/b"), XPATH, true);
    }

    @Test
    public void boostTitleOverDescription() throws Exception{
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");

        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);

        Tree title = props.addChild("title");
        title.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:content/jcr:title");
        title.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        title.setProperty(FulltextIndexConstants.FIELD_BOOST, 4.0);

        Tree desc = props.addChild("desc");
        desc.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:content/jcr:description");
        desc.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        desc.setProperty(FulltextIndexConstants.FIELD_BOOST, 2.0);

        Tree text = props.addChild("text");
        text.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:content/text");
        text.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = createNodeWithType(test, "a", "oak:TestNode").addChild("jcr:content");
        a.setProperty("jcr:title", "Batman");
        a.setProperty("jcr:description", "Silent angel of Gotham");
        a.setProperty("text", "once upon a time a long text phrase so as to add penalty to /test/a and nullifying boost");

        Tree b = createNodeWithType(test, "b", "oak:TestNode").addChild("jcr:content");
        b.setProperty("jcr:title", "Superman");
        b.setProperty("jcr:description", "Tale of two heroes Superman and Batman");
        b.setProperty("text", "some stuff");

        Tree c = createNodeWithType(test, "c", "oak:TestNode").addChild("jcr:content");
        c.setProperty("jcr:title", "Ironman");
        c.setProperty("jcr:description", "New kid in the town");
        c.setProperty("text", "Friend of batman?");
        root.commit();

        String queryString = "//element(*,oak:TestNode)[jcr:contains(., 'batman')]";
        String explain = explainXpath(queryString);

        //Assert that Lucene query generated has entries for all included boosted fields
        assertThat(explain, containsString("full:jcr:content/jcr:title:batman^4.0"));
        assertThat(explain, containsString("full:jcr:content/jcr:description:batman^2.0"));
        assertThat(explain, containsString(":fulltext:batman"));

        assertOrderedQuery(queryString, asList("/test/a", "/test/b", "/test/c"), XPATH, true);
    }

    @Test
    public void sortQueriesWithJcrScore() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "n0", "n1", "n2"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        for(int i = 3; i > 0; i--){
            Tree child = test.addChild("n" + i);
            child.setProperty("propa", "foo");
        }
        root.commit();

        // Descending matches with lucene native sort
        String query =
            "measure select [jcr:path] from [nt:base] where [propa] = 'foo' order by [jcr:score] desc";
        assertThat(measureWithLimit(query, SQL2, 1), containsString("scanCount: 1"));

        // Ascending needs to be sorted by query engine
        query =
            "measure select [jcr:path] from [nt:base] where [propa] = 'foo' order by [jcr:score]";
        assertThat(measureWithLimit(query, SQL2, 1), containsString("scanCount: 3"));
    }

    @Test
    public void sortFulltextQueriesWithJcrScore() throws Exception {
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa"));
        idx.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, true);
        useV2(idx);

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo");
        root.commit();

        // Descending matches with lucene native sort
        String query = "measure //*[jcr:contains(., 'foo' )] order by @jcr:score descending";
        assertThat(measureWithLimit(query, XPATH, 1), containsString("scanCount: 1"));

        // Ascending needs to be sorted by query engine
        query = "measure //*[jcr:contains(., 'foo' )] order by @jcr:score";
        assertThat(measureWithLimit(query, XPATH, 1), containsString("scanCount: 3"));
    }

    // OAK-2434
    private void fulltextBooleanComplexOrQueries(boolean ver2) throws Exception {
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        idx.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, true);
        if (ver2) {
            useV2(idx);
        }

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();
        Tree a = test.addChild("a");
        a.setProperty("propa", "fox is jumping");
        a.setProperty("propb", "summer is here");

        Tree b = test.addChild("b");
        b.setProperty("propa", "fox is sleeping");
        b.setProperty("propb", "winter is here");

        Tree c = test.addChild("c");
        c.setProperty("propa", "fox is jumping");
        c.setProperty("propb", "autumn is here");

        root.commit();
        assertQuery(
                "select * from [nt:base] where CONTAINS(*, 'fox') and CONTAINS([propb], '\"winter is here\" OR \"summer "
                        + "is here\"')",
                asList("/test/a", "/test/b"));
    }

    // OAK-2434
    @Test
    public void luceneBooleanComplexOrQueries() throws Exception {
        fulltextBooleanComplexOrQueries(false);
    }

    // OAK-2434
    @Test
    public void lucenPropertyBooleanComplexOrQueries() throws Exception {
        fulltextBooleanComplexOrQueries(true);
    }

    // OAK-2438
    @Test
    // Copied and modified slightly from org.apache.jackrabbit.core.query.FulltextQueryTest#testFulltextExcludeSQL
    public void luceneAndExclude() throws Exception {
        Tree indexDefn = createTestIndexNode(root.getTree("/"), LuceneIndexConstants.TYPE_LUCENE);
        Tree r = root.getTree("/").addChild("test");

        Tree n = r.addChild("node1");
        n.setProperty("title", "test text");
        n.setProperty("mytext", "the quick brown fox jumps over the lazy dog.");
        n = r.addChild("node2");
        n.setProperty("title", "other text");
        n.setProperty("mytext", "the quick brown fox jumps over the lazy dog.");
        root.commit();

        String sql = "SELECT * FROM [nt:base] WHERE [jcr:path] LIKE \'" + r.getPath() + "/%\'"
            + " AND CONTAINS(*, \'text \'\'fox jumps\'\' -other\')";
        assertQuery(sql, asList("/test/node1"));
    }

    private String measureWithLimit(String query, String lang, int limit) throws ParseException {
        List<? extends ResultRow> result = CollectionUtils.toList(
            qe.executeQuery(query, lang, limit, 0, Maps.<String, PropertyValue>newHashMap(),
                NO_MAPPINGS).getRows());

        String measure = "";
        if (result.size() > 0) {
            measure = result.get(0).toString();
        }
        return measure;
    }

    @Test
    public void indexTimeFieldBoostAndRelativeProperty() throws Exception {
        // Index Definition
        Tree index = root.getTree("/");
        Tree indexDefn = createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        useV2(indexDefn);

        addPropertyDefn(indexDefn, "jcr:content/metadata/title", 4.0);
        addPropertyDefn(indexDefn, "jcr:content/metadata/title2", 2.0);
        addPropertyDefn(indexDefn, "propa", 1.0);

        root.commit();

        // create test data
        Tree test = root.getTree("/").addChild("test");
        usc(test, "a").setProperty("propa", "foo foo foo");
        usc(test, "b").addChild("jcr:content").addChild("metadata").setProperty("title", "foo");
        usc(test, "c").addChild("jcr:content").addChild("metadata").setProperty("title2", "foo");
        root.commit();

        String queryString = "//element(*, oak:Unstructured)[jcr:contains(., 'foo' )]";
        // verify results ordering
        // which should be /test/c (boost = 4.0), /test/a(boost = 2.0), /test/b (1.0)
        assertOrderedQuery(queryString, asList("/test/b", "/test/c", "/test/a"), XPATH, true);
    }

    @Test
    public void customTikaConfig() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree test = root.getTree("/").addChild("test");
        createFileNode(test, "text", "fox is jumping", "text/plain");
        createFileNode(test, "xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><msg>sky is blue</msg>", "application/xml");
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'fox ')", asList("/test/text/jcr:content"));
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky ')", asList("/test/xml/jcr:content"));

        //Now disable extraction for application/xml and see that query
        //does not return any result for that
        idx = root.getTree("/oak:index/test");
        String tikaConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<properties>\n" +
                "  <detectors>\n" +
                "    <detector class=\"org.apache.tika.detect.DefaultDetector\"/>\n" +
                "  </detectors>\n" +
                "  <parsers>\n" +
                "    <parser class=\"org.apache.tika.parser.DefaultParser\"/>\n" +
                "    <parser class=\"org.apache.tika.parser.EmptyParser\">\n" +
                "      <mime>application/xml</mime>\n" +
                "    </parser>\n" +
                "  </parsers>\n" +
                "</properties>";

        idx.addChild(LuceneIndexConstants.TIKA)
                .addChild(LuceneIndexConstants.TIKA_CONFIG)
                .addChild(JCR_CONTENT)
                .setProperty(JCR_DATA, tikaConfig.getBytes());
        idx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'fox ')", asList("/test/text/jcr:content"));
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky ')", Collections.<String>emptyList());
    }

    @Test
    public void excludedBlobContentNotAccessed() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        AccessStateProvidingBlob testBlob =
                new AccessStateProvidingBlob("<?xml version=\"1.0\" encoding=\"UTF-8\"?><msg>sky is blue</msg>");

        Tree test = root.getTree("/").addChild("test");
        createFileNode(test, "zip", testBlob, "application/zip");
        root.commit();

        assertFalse(testBlob.isStreamAccessed());
        assertEquals(0, testBlob.readByteCount());
    }

    @Test
    public void preExtractedTextProvider() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);
        root.commit();

        AccessStateProvidingBlob testBlob =
                new AccessStateProvidingBlob("fox is jumping", "id1");

        MapBasedProvider textProvider = new MapBasedProvider();
        textProvider.write("id1","lion");
        editorProvider.getExtractedTextCache().setExtractedTextProvider(textProvider);

        Tree test = root.getTree("/").addChild("test");
        createFileNode(test, "text", testBlob, "text/plain");
        root.commit();

        //As its not a reindex case actual blob content would be accessed
        assertTrue(testBlob.isStreamAccessed());
        assertQuery("select * from [nt:base] where CONTAINS(*, 'fox ')", asList("/test/text/jcr:content"));
        assertEquals(0, textProvider.accessCount);

        testBlob.resetState();

        //Lets trigger a reindex
        root.getTree(idx.getPath()).setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        //Now the content should be provided by the PreExtractedTextProvider
        //and instead of fox its lion!
        assertFalse(testBlob.isStreamAccessed());
        assertQuery("select * from [nt:base] where CONTAINS(*, 'lion ')", asList("/test/text/jcr:content"));
        assertEquals(1, textProvider.accessCount);
    }

    @Test
    public void preExtractedTextCache() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);
        root.commit();

        AccessStateProvidingBlob testBlob =
                new AccessStateProvidingBlob("fox is jumping", "id1");

        //1. Check by adding blobs in diff commit and reset
        //cache each time. In such case blob stream would be
        //accessed as many times
        Tree test = root.getTree("/").addChild("test");
        createFileNode(test, "text", testBlob, "text/plain");
        root.commit();

        editorProvider.getExtractedTextCache().resetCache();

        test = root.getTree("/").addChild("test");
        createFileNode(test, "text2", testBlob, "text/plain");
        root.commit();

        assertTrue(testBlob.isStreamAccessed());
        assertEquals(2, testBlob.accessCount);

        //Reset all test state
        testBlob.resetState();
        editorProvider.getExtractedTextCache().resetCache();

        //2. Now add 2 nodes with same blob in same commit
        //This time cache effect would come and blob would
        //be accessed only once
        test = root.getTree("/").addChild("test");
        createFileNode(test, "text3", testBlob, "text/plain");
        createFileNode(test, "text4", testBlob, "text/plain");
        root.commit();

        assertTrue(testBlob.isStreamAccessed());
        assertEquals(1, testBlob.accessCount);

        //Reset
        testBlob.resetState();

        //3. Now just add another node with same blob with no cache
        //reset. This time blob stream would not be accessed at all
        test = root.getTree("/").addChild("test");
        createFileNode(test, "text5", testBlob, "text/plain");
        root.commit();

        assertFalse(testBlob.isStreamAccessed());
        assertEquals(0, testBlob.accessCount);
    }

    @Test
    public void maxFieldLengthCheck() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("text", "red brown fox was jumping");
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", asList("/test"));

        idx = root.getTree("/oak:index/test");
        idx.setProperty(LuceneIndexConstants.MAX_FIELD_LENGTH, 2);
        idx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", Collections.<String>emptyList());
    }

    @Test
    public void maxExtractLengthCheck() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree test = root.getTree("/").addChild("test");
        createFileNode(test, "text", "red brown fox was jumping", "text/plain");
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", asList("/test/text/jcr:content"));
        assertQuery("select * from [nt:base] where CONTAINS(*, 'red')", asList("/test/text/jcr:content"));

        idx = root.getTree("/oak:index/test");
        idx.addChild(TIKA).setProperty(LuceneIndexConstants.TIKA_MAX_EXTRACT_LENGTH, 15);
        idx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", Collections.<String>emptyList());
        assertQuery("select * from [nt:base] where CONTAINS(*, 'red')", asList("/test/text/jcr:content"));
    }

    @Test
    public void binaryNotIndexedWhenMimeTypeNull() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree test = root.getTree("/").addChild("test");
        String path = createFileNode(test, "text", "red brown fox was jumping", "text/plain").getPath();
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", asList("/test/text/jcr:content"));

        //Remove the mimeType property. Then binary would not be indexed and result would be empty
        root.getTree(path).removeProperty(JcrConstants.JCR_MIMETYPE);
        root.commit();
        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", Collections.<String>emptyList());
    }

    @Test
    public void binaryNotIndexedWhenNotSupportedMimeType() throws Exception{
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree test = root.getTree("/").addChild("test");
        String path = createFileNode(test, "text", "red brown fox was jumping", "text/plain").getPath();
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", asList("/test/text/jcr:content"));

        root.getTree(path).setProperty(JcrConstants.JCR_MIMETYPE, "foo/bar");
        root.commit();
        assertQuery("select * from [nt:base] where CONTAINS(*, 'jumping')", Collections.<String>emptyList());
    }

    @Test
    public void relativePropertyAndCursor() throws Exception{
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        TestUtil.useV2(idx);
        idx.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, true);

        Tree propNode = idx.addChild(PROP_NODE);

        // property definition for index test1
        Tree propA = propNode.addChild("propa");
        propA.setProperty(PROP_TYPE, PropertyType.TYPENAME_STRING);
        propA.setProperty(FulltextIndexConstants.FIELD_BOOST, 2.0);

        root.commit();

        // create test data with 1 more than batch size
        //with boost set we ensure that correct result comes *after* the batch size of results
        Tree test = root.getTree("/").addChild("test");
        root.commit();
        for (int i = 0; i < LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE; i++) {
            test.addChild("a"+i).addChild("doNotInclude").setProperty("propa", "foo");
        }
        test.addChild("b").addChild("jcr:content").setProperty("propb", "foo");
        root.commit();

        String queryString = "/jcr:root//element(*, nt:base)[jcr:contains(jcr:content, 'foo' )]";

        assertQuery(queryString, "xpath", asList("/test/b"));
    }

    @Test
    public void unionSortResultCount() throws Exception {
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa", "propb", "propc"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("propc"), STRINGS));
        useV2(idx);

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        List<Integer> nodes = new ArrayList<>();
        Random r = new Random();
        int seed = -2;
        for (int i = 0; i < 1000; i++) {
            Tree a = test.addChild("a" + i);
            a.setProperty("propa", "fooa");
            seed += 2;
            int num = r.nextInt(100);
            a.setProperty("propc", num);
            nodes.add(num);
        }

        seed = -1;
        for (int i = 0; i < 1000; i++) {
            Tree a = test.addChild("b" + i);
            a.setProperty("propb", "foob");
            seed += 2;
            int num = 100 + r.nextInt(100);
            a.setProperty("propc",  num);
            nodes.add(num);
        }
        root.commit();

        // scan count scans the whole result set
        String query =
            "measure /jcr:root//element(*, nt:base)[(@propa = 'fooa' or @propb = 'foob')] order by @propc";
        assertThat(measureWithLimit(query, XPATH, 100), containsString("scanCount: 101"));
    }


    @Test
    public void unionSortQueries() throws Exception {
        // Index Definition
        Tree idx = createIndex("test1", Set.of("propa", "propb", "propc", "propd"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("propd"), STRINGS));
        useV2(idx);

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        int seed = -3;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("a" + i);
            a.setProperty("propa", "a" + i);
            seed += 3;
            a.setProperty("propd", seed);
        }

        seed = -2;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("b" + i);
            a.setProperty("propb", "b" + i);
            seed += 3;
            a.setProperty("propd", seed);
        }
        seed = -1;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("c" + i);
            a.setProperty("propc", "c" + i);
            seed += 3;
            a.setProperty("propd", seed);
        }
        root.commit();

        assertQuery(
            "/jcr:root//element(*, nt:base)[(@propa = 'a4' or @propb = 'b3')] order by @propd",
            XPATH,
            asList("/test/b3", "/test/a4"));
        assertQuery(
            "/jcr:root//element(*, nt:base)[(@propa = 'a3' or @propb = 'b0' or @propc = 'c2')] order by @propd",
            XPATH,
            asList("/test/b0", "/test/c2", "/test/a3"));
    }

    @Test
    public void aggregationAndExcludeProperty() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(PROP_PROPERTY_INDEX, true);

        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "original/jcr:content/type");
        prop1.setProperty(FulltextIndexConstants.PROP_EXCLUDE_FROM_AGGREGATE, true);
        prop1.setProperty(PROP_PROPERTY_INDEX, true);

        newNodeAggregator(idx)
                .newRuleWithName(NT_FILE, newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"))
                .newRuleWithName(TestUtil.NT_TEST, newArrayList("/*"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = createNodeWithType(test, "a", TestUtil.NT_TEST);
        Tree af = createFileNode(a, "original", "hello", "text/plain");
        af.setProperty("type", "jpg"); //Should be excluded
        af.setProperty("class", "image"); //Should be included

        root.commit();

        // hello and image would be index by aggregation but
        // jpg should be exclude as there is a property defn to exclude it
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'hello')", asList("/test/a"));
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'image')", asList("/test/a"));
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'jpg')", Collections.<String>emptyList());

        //Check that property index is being used
        assertThat(explain("select [jcr:path] from [oak:TestNode] where [original/jcr:content/type] = 'foo'"),
                containsString("original/jcr:content/type:foo"));
    }

    @Test
    public void aggregateAndIncludeRelativePropertyByDefault() throws Exception{
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(PROP_PROPERTY_INDEX, true);

        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "original/jcr:content/type");
        prop1.setProperty(PROP_PROPERTY_INDEX, true);

        newNodeAggregator(idx)
                .newRuleWithName(NT_FILE, newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"))
                .newRuleWithName(TestUtil.NT_TEST, newArrayList("/*"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = createNodeWithType(test, "a", TestUtil.NT_TEST);
        Tree af = createFileNode(a, "original", "hello", "text/plain");
        af.setProperty("type", "jpg");
        af.setProperty("class", "image"); //Should be included

        root.commit();

        // hello and image would be index by aggregation but
        // jpg should also be included as it has not been excluded
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'hello')", asList("/test/a"));
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'image')", asList("/test/a"));
        assertQuery("select [jcr:path] from [oak:TestNode] where contains(*, 'jpg')", asList("/test/a"));

        //Check that property index is being used
        assertThat(explain("select [jcr:path] from [oak:TestNode] where [original/jcr:content/type] = 'foo'"),
                containsString("original/jcr:content/type:foo"));
    }

    @Test
    public void indexingBasedOnMixin() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithMixinType(test, "a", "mix:title").setProperty("jcr:title", "a");
        createNodeWithMixinType(test, "b", "mix:title").setProperty("jcr:title", "c");
        test.addChild("c").setProperty("jcr:title", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:title] where [jcr:title] = 'a'";
        assertThat(explain(propabQuery), containsString("/oak:index/test1"));
        assertQuery(propabQuery, asList("/test/a"));
    }

    @Test
    public void indexingBasedOnMixinWithInheritence() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:mimeType");
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:mimeType");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "nt:resource").setProperty("jcr:mimeType", "a");
        createNodeWithType(test, "b", "nt:resource").setProperty("jcr:mimeType", "c");
        test.addChild("c").setProperty("jcr:mimeType", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:mimeType] where [jcr:mimeType] = 'a'";
        assertThat(explain(propabQuery), containsString("/oak:index/test1"));
        assertQuery(propabQuery, asList("/test/a"));
    }

    @Test
    public void indexingPropertyWithAnalyzeButQueryWithWildcard() throws Exception {
        Tree index = root.getTree("/");
        Tree idx = index.addChild(INDEX_DEFINITIONS_NAME).addChild("test2");
        // not async, to speed up testing
        // idx.setProperty("async", "async");
        idx.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        // idx.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);
        idx.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        idx.setProperty(REINDEX_PROPERTY_NAME, true);
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop = props.addChild(TestUtil.unique("jcr:mimeType"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:mimeType");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        prop.setProperty(PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("jcr:mimeType", "1234");
        test.addChild("b").setProperty("other", "1234");
        test.addChild("c").setProperty("jcr:mimeType", "a");
        root.commit();

        String query;

        query = "/jcr:root/test//*[jcr:contains(@jcr:mimeType, '1234')]";
        assertThat(explainXpath(query), containsString("/oak:index/test2"));
        assertQuery(query, "xpath", asList("/test/a"));

        query = "/jcr:root/test//*[jcr:contains(., '1234')]";
        assertThat(explainXpath(query), containsString("no-index"));

        query = "/jcr:root/test//*[@jcr:mimeType = '1234']";
        assertThat(explainXpath(query), containsString("/oak:index/test2"));
        assertQuery(query, "xpath", asList("/test/a"));
    }

    @Ignore("OAK-4042")
    @Test
    public void gb18030FulltextSuffixQuery() throws Exception {
        String searchTerm1 = "normaltext";
        String searchTerm2 = "中文标题";
        String propValue = "some text having suffixed " + searchTerm1 + "suffix and " + searchTerm2 + "suffix.";

        Tree index = root.getTree("/");
        Tree idx = index.addChild(INDEX_DEFINITIONS_NAME).addChild("test2");
        idx.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        idx.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        idx.setProperty(REINDEX_PROPERTY_NAME, true);
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop = props.addChild(TestUtil.unique("text"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, "text");
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        prop.setProperty(PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", propValue);
        root.commit();

        String query;

        query = "SELECT * from [nt:base] WHERE CONTAINS([text], '" + searchTerm1 + "*')";
        assertQuery(query, SQL2, asList("/test/a"));

        query = "SELECT * from [nt:base] WHERE CONTAINS([text], '" + searchTerm2 + "*')";
        assertQuery(query, SQL2, asList("/test/a"));
    }


    @Test
    public void indexingBasedOnMixinAndRelativeProps() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(PROP_PROPERTY_INDEX, true);

        Tree prop2 = props.addChild(TestUtil.unique("prop"));
        prop2.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:content/type");
        prop2.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        Tree a = createNodeWithMixinType(test, "a", "mix:title");
        a.setProperty("jcr:title", "a");
        a.addChild("jcr:content").setProperty("type", "foo-a");

        Tree c = createNodeWithMixinType(test, "c", "mix:title");
        c.setProperty("jcr:title", "c");
        c.addChild("jcr:content").setProperty("type", "foo-c");

        test.addChild("c").setProperty("jcr:title", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:title] where [jcr:content/type] = 'foo-a'";
        assertThat(explain(propabQuery), containsString("/oak:index/test1"));
        assertQuery(propabQuery, asList("/test/a"));
    }

    @Test
    public void reindexWithCOWWithoutIndexPath() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        // force CoR
        executeQuery("select * from [mix:title] where [jcr:title] = 'x'", SQL2);

        assertNotNull(corDir);
        String localPathBeforeReindex = corDir;

        // CoW with re-indexing
        idx.setProperty("reindex", true);
        root.commit();

        assertNotNull(cowDir);
        String localPathAfterReindex = cowDir;

        assertNotEquals("CoW should write to different dir on reindexing", localPathBeforeReindex, localPathAfterReindex);
    }

    @Test
    public void uniqueIdInitializedInIndexing() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(PROP_PROPERTY_INDEX, true);
        root.commit();

        //Make some changes such incremental indexing happens
        root.getTree("/").addChild("a").setProperty("jcr:title", "foo");
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());
        IndexDefinition defn = new IndexDefinition(INITIAL_CONTENT, idxState, "/foo");

        //Check that with normal indexing uid gets initialized
        String uid = defn.getUniqueId();
        assertNotNull(defn.getUniqueId());

        //Now trigger a reindex
        idx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        root.commit();

        //Refetch the NodeState
        idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());
        defn = new IndexDefinition(INITIAL_CONTENT, idxState, "/foo");

        //Check that uid is also initialized in reindexing
        String uid2 = defn.getUniqueId();
        assertNotNull(defn.getUniqueId());
        assertNotEquals(uid, uid2);
    }

    @Test
    public void fulltextQueryWithSpecialChars() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "tag");
        prop1.setProperty(PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("tag", "stockphotography:business/business_abstract");
        Tree test2 = root.getTree("/").addChild("test2");
        test2.setProperty("tag", "foo!");
        root.getTree("/").addChild("test3").setProperty("tag", "a=b");
        root.getTree("/").addChild("test4").setProperty("tag", "c=d=e");
        root.commit();

        String propabQuery = "select * from [nt:base] where CONTAINS(tag, " +
                "'stockphotography:business/business_abstract')";
        assertPlanAndQuery(propabQuery, "/oak:index/test1", asList("/test"));

        String query2 = "select * from [nt:base] where CONTAINS(tag, 'foo!')";
        assertPlanAndQuery(query2, "/oak:index/test1", asList("/test2"));

        String query3 = "select * from [nt:base] where CONTAINS(tag, 'a=b')";
        assertPlanAndQuery(query3, "/oak:index/test1", asList("/test3"));

        String query4 = "select * from [nt:base] where CONTAINS(tag, 'c=d=e')";
        assertPlanAndQuery(query4, "/oak:index/test1", asList("/test4"));

    }

    @Test
    public void fulltextQueryWithRelativeProperty() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "jcr:content/metadata/comment");
        prop1.setProperty(PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("jcr:content").addChild("metadata").setProperty("comment", "taken in december");
        root.commit();

        String propabQuery = "select * from [nt:base] where CONTAINS([jcr:content/metadata/comment], 'december')";
        assertPlanAndQuery(propabQuery, "/oak:index/test1", asList("/test"));
    }

    @Test
    public void longRepExcerpt() throws Exception {
        Tree luceneIndex = createFullTextIndex(root.getTree("/"), "lucene");

        root.commit();

        StringBuilder s = new StringBuilder();
        for (int k = 0; k < 100; k++) {
            s.append("foo bar ").append(k).append(" ");
        }
        String text = s.toString();
        List<String> names = new LinkedList<String>();
        for (int j = 0; j < 30; j++) {
            Tree test = root.getTree("/").addChild("ex-test-" + j);
            for (int i = 0; i < 100; i++) {
                String name = "cont" + i;
                test.addChild(name).setProperty("text", text);
                names.add("/" + test.getName() + "/" + name);
            }
        }

        root.commit();

        String query;

        query = "SELECT [jcr:path],[rep:excerpt] from [nt:base] WHERE CONTAINS([text], 'foo')";
        assertQuery(query, SQL2, names);

        // execute the query again to assert the excerpts value of the first row
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> rowsIt = result.getRows().iterator();
        while (rowsIt.hasNext()) {
            ResultRow row = rowsIt.next();
            PropertyValue excerptValue = row.getValue("rep:excerpt");
            assertFalse("There is an excerpt expected for each result row for term 'foo'", excerptValue == null || "".equals(excerptValue.getValue(STRING)));
        }
    }

    @Test
    public void simpleRepExcerpt() throws Exception {
        createFullTextIndex(root.getTree("/"), "lucene");

        root.commit();

        Tree content = root.getTree("/").addChild("content");
        content.setProperty("foo", "Lorem ipsum, dolor sit", STRING);
        content.setProperty("bar", "dolor sit, luctus leo, ipsum", STRING);

        root.commit();

        String query = "SELECT [jcr:path],[rep:excerpt] from [nt:base] WHERE CONTAINS(*, 'ipsum')";

        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> resultRows = result.getRows().iterator();
        assertTrue(resultRows.hasNext());
        ResultRow firstRow = result.getRows().iterator().next();
        PropertyValue excerptValue = firstRow.getValue("rep:excerpt");
        assertTrue("There is an excerpt expected for rep:excerpt",excerptValue != null && !"".equals(excerptValue.getValue(STRING)));
        excerptValue = firstRow.getValue("rep:excerpt(.)");
        assertTrue("There is an excerpt expected for rep:excerpt(.)",excerptValue != null && !"".equals(excerptValue.getValue(STRING)));
        excerptValue = firstRow.getValue("rep:excerpt(bar)");
        assertTrue("There is an excerpt expected for rep:excerpt(bar) ",excerptValue != null && !"".equals(excerptValue.getValue(STRING)));
    }

    @Test
    public void emptySuggestDictionary() throws Exception{
        Tree idx = createIndex("test1", Set.of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(PROP_PROPERTY_INDEX, true);
        prop1.setProperty(FulltextIndexConstants.PROP_NAME, "tag");
        prop1.setProperty(FulltextIndexConstants.PROP_INDEX, true);
        prop1.setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
        root.commit();

        String query = "select * from [nt:base] where [tag] = 'foo'";
        assertPlanAndQuery(query, "/oak:index/test1", Collections.<String>emptyList());
    }

    @Test
    public void relativePropertyWithIndexOnNtBase() throws Exception {
        Tree idx = createIndex("test1", Set.of("propa"));
        idx.setProperty(PROP_TYPE, "lucene");
        useV2(idx);
        //Do not provide type information
        root.commit();

        Tree propTree = root.getTree(idx.getPath() + "/indexRules/nt:base/properties/propa");
        propTree.setProperty(PROP_ANALYZED, true);
        root.commit();

        Tree rootTree = root.getTree("/");
        Tree node1Tree = rootTree.addChild("node1");
        node1Tree.setProperty("propa", "abcdef");
        node1Tree.setProperty("propb", "abcdef");
        Tree node2Tree = rootTree.addChild("node2");
        node2Tree.setProperty("propa", "abc_def");
        node2Tree.setProperty("propb", "abc_def");
        root.commit();

        String query = "select [jcr:path] from [nt:base] where contains('propb', 'abc*')";
        String explanation = explain(query);
        assertThat(explanation, not(containsString("lucene:test1")));
    }

    @Test
    public void subNodeTypes() throws Exception{
        optionalEditorProvider.delegate = new TypeEditorProvider();
        String testNodeTypes =
                "[oak:TestMixA]\n" +
                "  mixin\n" +
                "\n" +
                "[oak:TestSuperType] \n" +
                " - * (UNDEFINED) multiple\n" +
                "\n" +
                "[oak:TestTypeA] > oak:TestSuperType\n" +
                " - * (UNDEFINED) multiple\n" +
                "\n" +
                " [oak:TestTypeB] > oak:TestSuperType, oak:TestMixA\n" +
                " - * (UNDEFINED) multiple\n" +
                "\n" +
                "  [oak:TestTypeC] > oak:TestMixA\n" +
                " - * (UNDEFINED) multiple";
        NodeTypeRegistry.register(root, IOUtils.toInputStream(testNodeTypes, "utf-8"), "test nodeType");
        //Flush the changes to nodetypes
        root.commit();

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("oak:TestSuperType").property(JcrConstants.JCR_PRIMARYTYPE).propertyIndex();
        idxb.indexRule("oak:TestMixA").property(JcrConstants.JCR_MIXINTYPES).propertyIndex();
        idxb.indexRule("oak:TestMixA").property(JcrConstants.JCR_PRIMARYTYPE).propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.getTree("/oak:index/nodetype").remove();

        Tree rootTree = root.getTree("/");
        createNodeWithType(rootTree, "a", "oak:TestTypeA");
        createNodeWithType(rootTree, "b", "oak:TestTypeB");
        createNodeWithMixinType(rootTree, "c", "oak:TestMixA")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE,  "oak:Unstructured", Type.NAME);

        root.commit();

        assertPlanAndQuery("select * from [oak:TestSuperType]", "/oak:index/test1", asList("/a", "/b"));
        assertPlanAndQuery("select * from [oak:TestMixA]", "/oak:index/test1", asList("/b", "/c"));
    }

    @Test
    public void subNodeTypes_nodeTypeIndex() throws Exception{
        optionalEditorProvider.delegate = new TypeEditorProvider();
        String testNodeTypes =
                "[oak:TestMixA]\n" +
                        "  mixin\n" +
                        "\n" +
                        "[oak:TestSuperType] \n" +
                        " - * (UNDEFINED) multiple\n" +
                        "\n" +
                        "[oak:TestTypeA] > oak:TestSuperType\n" +
                        " - * (UNDEFINED) multiple\n" +
                        "\n" +
                        " [oak:TestTypeB] > oak:TestSuperType, oak:TestMixA\n" +
                        " - * (UNDEFINED) multiple\n" +
                        "\n" +
                        "  [oak:TestTypeC] > oak:TestMixA\n" +
                        " - * (UNDEFINED) multiple";
        NodeTypeRegistry.register(root, IOUtils.toInputStream(testNodeTypes, "utf-8"), "test nodeType");
        //Flush the changes to nodetypes
        root.commit();

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.nodeTypeIndex();
        idxb.indexRule("oak:TestSuperType");
        idxb.indexRule("oak:TestMixA");

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.getTree("/oak:index/nodetype").remove();

        Tree rootTree = root.getTree("/");
        createNodeWithType(rootTree, "a", "oak:TestTypeA");
        createNodeWithType(rootTree, "b", "oak:TestTypeB");
        createNodeWithMixinType(rootTree, "c", "oak:TestMixA")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE,  "oak:Unstructured", Type.NAME);

        root.commit();

        assertPlanAndQuery("select * from [oak:TestSuperType]", "/oak:index/test1", asList("/a", "/b"));
        assertPlanAndQuery("select * from [oak:TestMixA]", "/oak:index/test1", asList("/b", "/c"));
    }


    @Test
    public void indexDefinitionModifiedPostReindex() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        String query = "select * from [nt:base] where [foo] = 'bar'";
        assertPlanAndQuery(query, "/oak:index/test1", asList("/a"));

        Tree barProp = root.getTree("/oak:index/test1/indexRules/nt:base/properties").addChild("bar");
        barProp.setProperty("name", "bar");
        barProp.setProperty("propertyIndex", true);
        root.commit();

        query = "select * from [nt:base] where [bar] = 'bar'";
        assertThat(explain(query), not(containsString("/oak:index/test1")));

        root.getTree("/oak:index/test1").setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertPlanAndQuery(query, "/oak:index/test1", asList("/b"));
    }

    @Test
    public void refreshIndexDefinition() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        String query = "select * from [nt:base] where [foo] = 'bar'";
        assertPlanAndQuery(query, "/oak:index/test1", asList("/a"));

        Tree barProp = root.getTree("/oak:index/test1/indexRules/nt:base/properties").addChild("bar");
        barProp.setProperty("name", "bar");
        barProp.setProperty("propertyIndex", true);
        root.commit();

        query = "select * from [nt:base] where [bar] = 'bar'";
        assertThat(explain(query), not(containsString("/oak:index/test1")));

        //Instead of reindex just refresh the index definition so that new index definition gets picked up
        root.getTree("/oak:index/test1").setProperty(PROP_REFRESH_DEFN, true);
        root.commit();

        //Plan would reflect new defintion
        assertThat(explain(query), containsString("/oak:index/test1"));
        assertFalse(root.getTree("/oak:index/test1").hasProperty(PROP_REFRESH_DEFN));

        //However as reindex was not done query would result in empty set
        assertPlanAndQuery(query, "/oak:index/test1", Collections.<String>emptyList());
    }

    @Test
    public void updateOldIndexDefinition() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        //Cannot use Tree api as it cannot read hidden tree
        String clonedDefnPath = "/oak:index/test1/" + INDEX_DEFINITION_NODE;
        assertTrue(NodeStateUtils.getNode(nodeStore.getRoot(), clonedDefnPath).exists());

        NodeBuilder builder = nodeStore.getRoot().builder();
        child(builder, clonedDefnPath).remove();
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        root.rebase();
        rootTree = root.getTree("/");
        rootTree.addChild("c").setProperty("foo", "bar");
        root.commit();

        //Definition state should be recreated
        assertTrue(NodeStateUtils.getNode(nodeStore.getRoot(), clonedDefnPath).exists());
    }

    @Test
    public void disableIndexDefnStorage() throws Exception{
        IndexDefinition.setDisableStoredIndexDefinition(true);

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        String clonedDefnPath = "/oak:index/test1/" + INDEX_DEFINITION_NODE;
        assertFalse(NodeStateUtils.getNode(nodeStore.getRoot(), clonedDefnPath).exists());
    }

    @Test
    public void storedIndexDefinitionDiff() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        idx.setProperty(PROP_RANDOM_SEED, new Random().nextLong());
        root.commit();

        AsyncIndexInfoService asyncService = new AsyncIndexInfoServiceImpl(nodeStore);
        LuceneIndexInfoProvider indexInfoProvider = new LuceneIndexInfoProvider(nodeStore, asyncService, temporaryFolder.newFolder());

        IndexInfo info = indexInfoProvider.getInfo("/oak:index/test1");
        assertNotNull(info);

        assertFalse(info.hasIndexDefinitionChangedWithoutReindexing());
        assertNull(info.getIndexDefinitionDiff());

        Tree idxTree = root.getTree("/oak:index/test1");
        idxTree.setProperty("foo", "bar");
        root.commit();

        info = indexInfoProvider.getInfo("/oak:index/test1");
        assertTrue(info.hasIndexDefinitionChangedWithoutReindexing());
        assertNotNull(info.getIndexDefinitionDiff());
    }

    @Test
    public void relativeProperties() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("jcr:content").setProperty("foo", "bar");
        rootTree.addChild("b").addChild("jcr:content").setProperty("foo", "bar");
        rootTree.addChild("c").setProperty("foo", "bar");
        rootTree.addChild("d").addChild("jcr:content").addChild("metadata").addChild("sub").setProperty("foo", "bar");

        root.commit();

        assertPlanAndQuery("select * from [nt:base] where [jcr:content/foo] = 'bar'",
                "/oak:index/test1", asList("/a", "/b"));

        assertPlanAndQuery("select * from [nt:base] where [jcr:content/metadata/sub/foo] = 'bar'",
                "/oak:index/test1", asList("/d"));
    }

    @Test
    public void testRepSimilarWithBinaryFeatureVectors() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = new LinkedList<>();
            int i = 0;
            for (String s : split) {
                if (i > 0) {
                    values.add(Double.parseDouble(s));
                }
                i++;
            }

            byte[] bytes = SimSearchUtils.toByteArray(values);
            List<Double> actual = SimSearchUtils.toDoubles(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    @Test
    public void testRepSimilarWithBinaryFeatureVectorsWithIndexSimilarityBinariesDefinedAsLucene() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync().indexSimilarityBinaries("lucene");
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = new LinkedList<>();
            int i = 0;
            for (String s : split) {
                if (i > 0) {
                    values.add(Double.parseDouble(s));
                }
                i++;
            }

            byte[] bytes = SimSearchUtils.toByteArray(values);
            List<Double> actual = SimSearchUtils.toDoubles(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    /**
     * To disable similarity for binaries the index type should not be in present as value for FulltextIndexConstants.INDEX_SIMILARITY_BINARIES.
     * In this case index type is lucene but indexSimilarityBinaries is set to elasticsearch
     *
     * @throws Exception
     */
    @Test
    public void testRepSimilarWithBinaryFeatureVectorsWithIndexSimilarityBinariesDefinedAsElasticsearch() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync().indexSimilarityBinaries("elasticsearch");
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();


        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = new LinkedList<>();
            int i = 0;
            for (String s : split) {
                if (i > 0) {
                    values.add(Double.parseDouble(s));
                }
                i++;
            }

            byte[] bytes = SimSearchUtils.toByteArray(values);
            List<Double> actual = SimSearchUtils.toDoubles(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertEquals("binary data for similarity should not be indexed", 0, current.size());
        }
    }

    @Test
    public void testRepSimilarWithStringFeatureVectors() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();

        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            int i1 = line.indexOf(',');
            String name = line.substring(0, i1);
            String value = line.substring(i1 + 1);
            Tree child = test.addChild(name);
            child.setProperty("fv", value, Type.STRING);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    @Test
    public void testRepSimilarWithStringFeatureVectorsWithIndexSimilarityStringsDefinedAsLucene() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync().indexSimilarityStrings("lucene");
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();

        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            int i1 = line.indexOf(',');
            String name = line.substring(0, i1);
            String value = line.substring(i1 + 1);
            Tree child = test.addChild(name);
            child.setProperty("fv", value, Type.STRING);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    /**
     * To disable similarity for strings the index type should not be in present as value for FulltextIndexConstants.INDEX_SIMILARITY_STRINGS.
     * In this case index type is lucene but indexSimilarityStrings is set to elasticsearch
     *
     * @throws Exception
     */
    @Test
    public void testRepSimilarWithStringFeatureVectorsWithIndexSimilarityStringsDefinedAsElasticsearch() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync().indexSimilarityStrings("elasticsearch");
        idxb.indexRule("nt:base").property("fv").useInSimilarity().nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();

        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            int i1 = line.indexOf(',');
            String name = line.substring(0, i1);
            String value = line.substring(i1 + 1);
            Tree child = test.addChild(name);
            child.setProperty("fv", value, Type.STRING);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertEquals("String data for similarity should not be indexed", 0, current.size());
        }
    }

    @Test
    public void testRepSimilarWithBinaryFeatureVectorsAndRerank() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = new LinkedList<>();
            int i = 0;
            for (String s : split) {
                if (i > 0) {
                    values.add(Double.parseDouble(s));
                }
                i++;
            }

            byte[] bytes = SimSearchUtils.toByteArray(values);
            List<Double> actual = SimSearchUtils.toDoubles(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    @Test
    public void testRepSimilarWithStringFeatureVectorsAndRerank() throws Exception {

        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex().propertyIndex();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();


        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();

        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            int i1 = line.indexOf(',');
            String name = line.substring(0, i1);
            String value = line.substring(i1 + 1);
            Tree child = test.addChild(name);
            child.setProperty("fv", value, Type.STRING);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }
    }

    @Test
    public void injectRandomSeedDuringReindex() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.commit();

        // Push a change to get another indexing cycle run
        root.getTree("/").addChild("force-index-run");
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());

        long defSeed = idxState.getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);
        long clonedSeed = idxState.getChildNode(INDEX_DEFINITION_NODE).getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);

        assertTrue("Injected def (" + defSeed + ")and clone (" + clonedSeed + " seeds aren't same", defSeed == clonedSeed);
    }

    @Test
    public void injectRandomSeedDuringRefresh() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.commit();

        long seed = new Random().nextLong();

        // Push a change to get another indexing cycle run
        idx.setProperty(PROP_RANDOM_SEED, seed);
        idx.setProperty(PROP_REFRESH_DEFN, true);
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());

        long defSeed = idxState.getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);
        long clonedSeed = idxState.getChildNode(INDEX_DEFINITION_NODE).getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);

        assertEquals("Random seed not updated", seed, defSeed);
        assertTrue("Injected def (" + defSeed + ")and clone (" + clonedSeed + " seeds aren't same", defSeed == clonedSeed);
    }

    @Test
    public void injectRandomSeedDuringUpdate() throws Exception{
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.commit();

        long seed = new Random().nextLong();

        // Update seed
        idx.setProperty(PROP_RANDOM_SEED, seed);
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());

        long defSeed = idxState.getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);
        long clonedSeed = idxState.getChildNode(INDEX_DEFINITION_NODE).getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);

        assertEquals("Random seed not updated", seed, defSeed);
        assertTrue("Injected def (" + defSeed + ")and clone (" + clonedSeed + " seeds aren't same", defSeed == clonedSeed);
    }

    @Test
    public void injectGarbageSeed() throws Exception {
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.commit();

        long orignalSeed = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath())
                .getProperty(PROP_RANDOM_SEED)
                .getValue(Type.LONG);

        // Update seed
        idx.setProperty(PROP_RANDOM_SEED, "garbage");
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());

        long defSeed = idxState.getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);
        long clonedSeed = idxState.getChildNode(INDEX_DEFINITION_NODE).getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);

        assertNotEquals("Random seed not updated", orignalSeed, defSeed);
        assertNotEquals("Random seed updated to garbage", "garbage", defSeed);
        assertTrue("Injected def (" + defSeed + ")and clone (" + clonedSeed + " seeds aren't same", defSeed == clonedSeed);
    }

    @Test
    public void injectStringLongSeed() throws Exception {
        IndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        root.commit();
        // Update seed
        idx.setProperty(PROP_RANDOM_SEED, "-12312");
        root.commit();

        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), idx.getPath());

        long defSeed = idxState.getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);
        long clonedSeed = idxState.getChildNode(INDEX_DEFINITION_NODE).getProperty(PROP_RANDOM_SEED).getValue(Type.LONG);

        assertEquals("Random seed udpated to garbage", -12312, defSeed);
        assertTrue("Injected def (" + defSeed + ")and clone (" + clonedSeed + " seeds aren't same", defSeed == clonedSeed);
    }

    @Test
    public void pathTransformationWithWildcardInRelativePathFragment() throws Exception {
        IndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder().noAsync();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("fooIndex");
        idxBuilder.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("j:c").addChild("foo1").setProperty("foo", "bar");
        rootTree.addChild("b").addChild("j:c").addChild("foo2").setProperty("foo", "bar");
        rootTree.addChild("c").addChild("j:c").setProperty("foo", "bar");
        rootTree.addChild("d").addChild("e").addChild("j:c").addChild("foo3").setProperty("foo", "bar");
        rootTree.addChild("j:c").addChild("foo4").setProperty("foo", "bar");//a document which doesn't have 2 levels above the property
        rootTree.addChild("foo5").setProperty("foo", "bar");//a document which doesn't have 2 levels above the property
        rootTree.addChild("f").setProperty("foo", "bar");//a document which doesn't have 2 levels above the property
        root.commit();

        // XPaths
        assertPlanAndQueryXPath("//*[j:c/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQueryXPath("//*[e/j:c/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/d"));

        assertPlanAndQueryXPath("//*[*/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQueryXPath("//*[*/@foo = 'bar']",
                "/oak:index/fooIndex",
                asList("/a/j:c", "/b/j:c", "/c", "/d/e/j:c", "/j:c", "/"));

        assertPlanAndQueryXPath("//*[j:c/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQueryXPath("//*[*/foo1/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/a"));

        assertPlanAndQueryXPath("//*[*/*/foo3/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/d"));

        // SQL2s
        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [j:c/*/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [e/j:c/*/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/d"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [*/*/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [*/foo] = 'bar'",
                "/oak:index/fooIndex",
                asList("/a/j:c", "/b/j:c", "/c", "/d/e/j:c", "/j:c", "/"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [j:c/*/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/a", "/b", "/d/e", "/"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [*/foo1/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/a"));

        assertPlanAndQuery("SELECT * FROM [nt:base] WHERE [*/*/foo3/foo] = 'bar'",
                "/oak:index/fooIndex", asList("/d"));
    }

    @Test
    public void pathTransformationWithEvaluatePathRestriction() throws Exception {
        // This test might feel redundant in presence of LuceneIndexPathRestrictionTest, BUT, this test
        // gets result via query engine so the results is exactly correct. While in LuceneIndexPathRestrictionTest
        // we're interested in how many constraint could the index resolve.. so those results would be super-set of
        // what we see here

        IndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder()
                .noAsync().evaluatePathRestrictions();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("fooIndex");
        idxBuilder.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/").addChild("test");
        rootTree.addChild("a").addChild("j:c").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("foo", "bar");
        rootTree.addChild("c").addChild("d").addChild("j:c").setProperty("foo", "bar");
        root.commit();

        // no path restriction
        assertPlanAndQueryXPath("//*[j:c/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a", "/test/c/d"));
        assertPlanAndQueryXPath("//*[*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a", "/test", "/test/c/d"));
        assertPlanAndQueryXPath("//*[d/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/c"));

        // any descendant
        assertPlanAndQueryXPath("/jcr:root/test//*[j:c/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a", "/test/c/d"));
        assertPlanAndQueryXPath("/jcr:root/test//*[*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a", "/test/c/d"));
        assertPlanAndQueryXPath("/jcr:root/test//*[d/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/c"));

        // direct children
        assertPlanAndQueryXPath("/jcr:root/test/*[j:c/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a"));
        assertPlanAndQueryXPath("/jcr:root/test/*[*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a"));
        assertPlanAndQueryXPath("/jcr:root/test/*[d/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/c"));

        // exact path
        assertPlanAndQueryXPath("/jcr:root/test/a[j:c/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a"));
        assertPlanAndQueryXPath("/jcr:root/test/a[*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/a"));
        assertPlanAndQueryXPath("/jcr:root/test/c[d/*/@foo = 'bar']",
                "/oak:index/fooIndex", asList("/test/c"));
    }

    private void assertPlanAndQueryXPath(String query, String planExpectation, List<String> paths) throws ParseException {
        assertXpathPlan(query, planExpectation);
        assertQuery(query, XPATH, paths, false);
    }
    private void assertPlanAndQuery(String query, String planExpectation, List<String> paths) {
        assertPlanAndQuery(query, planExpectation, paths, false);
    }
    private void assertPlanAndQuery(String query, String planExpectation, List<String> paths, boolean ordered){
        assertPlan(query, planExpectation);
        assertQuery(query, SQL2, paths, ordered);
    }

    private static Tree createNodeWithMixinType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singleton(typeName), Type.NAMES);
        return t;
    }

    private Tree createFileNode(Tree tree, String name, String content, String mimeType){
        return createFileNode(tree, name, new ArrayBasedBlob(content.getBytes()), mimeType);
    }

    private Tree createFileNode(Tree tree, String name, Blob content, String mimeType){
        return TestUtil.createFileNode(tree, name, content, mimeType);
    }

    private Tree usc(Tree parent, String childName){
        Tree child = parent.addChild(childName);
        child.setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:Unstructured", Type.NAME);
        return child;
    }

    private Tree addPropertyDefn(Tree indexDefn, String propName, double boost){
        Tree props = TestUtil.newRulePropTree(indexDefn, "oak:Unstructured");
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(FulltextIndexConstants.PROP_NAME, propName);
        prop.setProperty(PROP_PROPERTY_INDEX, true);
        prop.setProperty(PROP_ANALYZED, true);
        prop.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        prop.setProperty(FulltextIndexConstants.FIELD_BOOST, boost);
        return prop;
    }

    private void assertOrderedQuery(String sql, List<String> paths) {
        assertOrderedQuery(sql, paths, SQL2, false);
    }

    private void assertOrderedQuery(String sql, List<String> paths, String language, boolean skipSort) {
        List<String> result = executeQuery(sql, language, true, skipSort);
        assertEquals(paths, result);
    }

    //TODO Test for range with Date. Check for precision

    private void assertPlan(String query, String planExpectation) {
        assertThat(explain(query), containsString(planExpectation));
    }

    private void assertXpathPlan(String query, String planExpectation) throws ParseException {
        assertThat(explainXpath(query), containsString(planExpectation));
    }

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        return row.getValue("plan").getValue(Type.STRING);
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    public static Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    private Tree createFullTextIndex(Tree index, String name) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree props = def.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(PROP_NODE)
                .addChild("allProps");

        props.setProperty(PROP_ANALYZED, true);
        props.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        props.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true);
        props.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        props.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
        return def;
    }

    private static String dt(String date) throws ParseException {
        return String.format("CAST ('%s' AS DATE)",ISO8601.format(createCal(date)));
    }

    private static List<String> getSortedPaths(List<Tuple> tuples, OrderDirection dir) {
        if (OrderDirection.DESC == dir) {
            Collections.sort(tuples, Collections.reverseOrder());
        } else {
            Collections.sort(tuples);
        }
        List<String> paths = new ArrayList<>(tuples.size());
        for (Tuple t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    static List<String> getSortedPaths(List<Tuple2> tuples) {
        Collections.sort(tuples);
        List<String> paths = new ArrayList<>(tuples.size());
        for (Tuple2 t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    static List<Long> createLongs(int n){
        List<Long> values = new ArrayList<>(n);
        for (long i = 0; i < n; i++){
            values.add(i);
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Double> createDoubles(int n){
        Random rnd = new Random();
        List<Double> values = new ArrayList<>(n);
        for (long i = 0; i < n; i++){
            values.add(rnd.nextDouble());
        }
        Collections.shuffle(values);
        return values;
    }

    static List<String> createStrings(int n){
        List<String> values = new ArrayList<>(n);
        for (long i = 0; i < n; i++){
            values.add(String.format("value%04d",i));
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Calendar> createDates(int n) throws ParseException {
        Random rnd = new Random();
        List<Calendar> values = new ArrayList<>(n);
        for (long i = 0; i < n; i++){
            values.add(createCal(String.format("%02d/%02d/2%03d", rnd.nextInt(26) + 1, rnd.nextInt(10) + 1,i)));
        }
        Collections.shuffle(values);
        return values;
    }

    private static class Tuple implements Comparable<Tuple>{
        final Comparable value;
        final String path;

        private Tuple(Comparable value, String path) {
            this.value = value;
            this.path = path;
        }

        @Override
        public int compareTo(Tuple o) {
            return value.compareTo(o.value);
        }

        @Override
        public String toString() {
            return "Tuple{" +
                    "value=" + value +
                    ", path='" + path + '\'' +
                    '}';
        }
    }

    static class Tuple2 implements Comparable<Tuple2>{
        final Comparable value;
        final Comparable value2;
        final String path;

        public Tuple2(Comparable value, Comparable value2, String path) {
            this.value = value;
            this.value2 = value2;
            this.path = path;
        }

        @Override
        public int compareTo(Tuple2 o) {
            return ComparisonChain.start()
                    .compare(value, o.value)
                    .compare(value2, o.value2, Collections.reverseOrder())
                    .result();
        }

        @Override
        public String toString() {
            return "Tuple2{" +
                    "value=" + value +
                    ", value2=" + value2 +
                    ", path='" + path + '\'' +
                    '}';
        }
    }

    private static class AccessStateProvidingBlob extends ArrayBasedBlob {
        private CountingInputStream stream;
        private String id;
        private int accessCount;

        public AccessStateProvidingBlob(byte[] value) {
            super(value);
        }

        public AccessStateProvidingBlob(String content) {
            this(content.getBytes(StandardCharsets.UTF_8));
        }

        public AccessStateProvidingBlob(String content, String id) {
            this(content.getBytes(StandardCharsets.UTF_8));
            this.id = id;
        }

        @NotNull
        @Override
        public InputStream getNewStream() {
            accessCount++;
            stream = new CountingInputStream(super.getNewStream());
            return stream;
        }

        public boolean isStreamAccessed() {
            return stream != null;
        }

        public void resetState(){
            stream = null;
            accessCount = 0;
        }

        public long readByteCount(){
            if (stream == null){
                return 0;
            }
            return stream.getCount();
        }

        @Override
        public String getContentIdentity() {
            return id;
        }
    }

    private static class MapBasedProvider implements PreExtractedTextProvider {
        final Map<String, ExtractedText> idMap = Maps.newHashMap();
        int accessCount = 0;

        @Override
        public ExtractedText getText(String propertyPath, Blob blob) throws IOException {
            ExtractedText result = idMap.get(blob.getContentIdentity());
            if (result != null){
                accessCount++;
            }
            return result;
        }

        public void write(String id, String text){
            idMap.put(id, new ExtractedText(ExtractionResult.SUCCESS, text));
        }

        public void reset(){
            accessCount = 0;
        }
    }
}
