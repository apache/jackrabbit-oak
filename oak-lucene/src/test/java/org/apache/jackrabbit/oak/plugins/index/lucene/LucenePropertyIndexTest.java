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

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.QUERY_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.PathFilter.PROP_INCLUDED_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANALYZERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_ORIGINAL_TERM;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE_SCOPE_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TIKA;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorTest.createCal;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newNodeAggregator;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Charsets;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CountingInputStream;
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
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
        nodeStore = new MemoryNodeStore();
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot()) {
                @Override
                public Directory wrapForRead(String indexPath, IndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(IndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName);
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

    @Test
    public void fulltextSearchWithCustomAnalyzer() throws Exception {
        Tree idx = createFulltextIndex(root.getTree("/"), "test");
        TestUtil.useV2(idx);

        Tree anl = idx.addChild(LuceneIndexConstants.ANALYZERS).addChild(LuceneIndexConstants.ANL_DEFAULT);
        anl.addChild(LuceneIndexConstants.ANL_TOKENIZER).setProperty(LuceneIndexConstants.ANL_NAME, "whitespace");
        anl.addChild(LuceneIndexConstants.ANL_FILTERS).addChild("stop");

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("foo", "fox jumping");
        root.commit();

        assertQuery("select * from [nt:base] where CONTAINS(*, 'fox was jumping')", asList("/test"));
    }

    //OAK-4805
    @Test
    public void badIndexDefinitionShouldLetQEWork() throws Exception {
        Tree idx = createFulltextIndex(root.getTree("/"), "badIndex");
        TestUtil.useV2(idx);

        //This would allow index def to get committed. Else bad index def can't be created.
        idx.setProperty(ASYNC_PROPERTY_NAME, "async");

        Tree anl = idx.addChild(LuceneIndexConstants.ANALYZERS).addChild(LuceneIndexConstants.ANL_DEFAULT);
        anl.addChild(LuceneIndexConstants.ANL_TOKENIZER).setProperty(LuceneIndexConstants.ANL_NAME, "Standard");
        Tree synFilter = anl.addChild(LuceneIndexConstants.ANL_FILTERS).addChild("Synonym");
        synFilter.setProperty("synonyms", "syn.txt");
        // Don't add syn.txt to make analyzer (and hence index def) invalid
        // synFilter.addChild("syn.txt").addChild(JCR_CONTENT).setProperty(JCR_DATA, "blah, foo, bar");
        root.commit();

        //Using this version of executeQuery as we don't want a result row quoting the exception
        executeQuery("SELECT * FROM [nt:base] where a='b'", SQL2, NO_BINDINGS);
    }

    @Test
    public void testSynonyms() throws Exception {
        Tree idx = createFulltextIndex(root.getTree("/"), "synonymIndex");
        TestUtil.useV2(idx);

        Tree anl = idx.addChild(LuceneIndexConstants.ANALYZERS).addChild(LuceneIndexConstants.ANL_DEFAULT);
        anl.addChild(LuceneIndexConstants.ANL_TOKENIZER).setProperty(LuceneIndexConstants.ANL_NAME, "Standard");
        Tree synFilter = anl.addChild(LuceneIndexConstants.ANL_FILTERS).addChild("Synonym");
        synFilter.setProperty("synonyms", "syn.txt");
        synFilter.addChild("syn.txt").addChild(JCR_CONTENT).setProperty(JCR_DATA, "plane, airplane, aircraft\nflies=>scars");

        Tree test = root.getTree("/").addChild("test").addChild("node");
        test.setProperty("foo", "an aircraft flies");
        root.commit();

        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'plane')", asList("/test/node"));
        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'airplane')", asList("/test/node"));
        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'aircraft')", asList("/test/node"));
        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'scars')", asList("/test/node"));
    }

    private Tree createFulltextIndex(Tree index, String name) throws CommitFailedException {
        return TestUtil.createFulltextIndex(index, name);
    }

    @Test
    public void indexSelection() throws Exception {
        createIndex("test1", of("propa", "propb"));
        createIndex("test2", of("propc"));

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
        Tree luceneIndex = createIndex("test1", of("propa"));
        // decrease cost of lucene property index
        luceneIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 5L, Type.LONG);

        // Decrease cost of node type index
        Tree nodeTypeIndex = root.getTree("/").getChild("oak:index").getChild("nodetype");
        nodeTypeIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 50L, Type.LONG);
        nodeTypeIndex.setProperty(IndexConstants.KEY_COUNT_PROPERTY_NAME, 10L, Type.LONG);

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
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
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, of("nt:file"), NAMES));
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
        System.out.println(explainXpath(propabQuery));
        assertThat(explainXpath(propabQuery), containsString("nodeType"));
    }

    @Test
    public void declaringNodeTypeSameProp() throws Exception {
        createIndex("test1", of("propa"));

        Tree indexWithType = createIndex("test2", of("propa"));
        indexWithType.setProperty(PropertyStates
                .createProperty(DECLARING_NODE_TYPES, of("nt:unstructured"),
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
        Tree indexWithType = createIndex("test2", of("propa", "propb"));
        indexWithType.setProperty(PropertyStates
            .createProperty(DECLARING_NODE_TYPES, of("nt:unstructured"),
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
        assertQuery(propNoIdxQuery, ImmutableList.<String>of());
    }

    @Test
    public void usedAsNodeTypeIndex() throws Exception {
        Tree nodeTypeIdx = root.getTree("/oak:index/nodetype");
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, of("nt:resource"), NAMES));
        nodeTypeIdx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        Tree indexWithType = createIndex("test2", of(JcrConstants.JCR_PRIMARYTYPE, "propb"));
        indexWithType.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, of("nt:file"), NAMES));

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
        nodeTypeIdx.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, of("nt:resource"), NAMES));
        nodeTypeIdx.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        Tree indexWithType = createIndex("test2", of("propb"));
        indexWithType.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, of("nt:file"), NAMES));
        indexWithType.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree rules = idx.addChild(LuceneIndexConstants.INDEX_RULES);
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
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1) :nodeName:foo"));
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
        Tree rules = idx.addChild(LuceneIndexConstants.INDEX_RULES);
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
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1) :nodeName:foo"));
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"), containsString("lucene:test1"));
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b"));
    }

    @Test
    public void explainScoreTest() throws Exception {
        Tree idx = createIndex("test1", of("propa"));
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
        Tree idx = createIndex("test1", of("tags"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("tags", of("a", "b"), Type.STRINGS);
        test.addChild("b").setProperty("tags", of("a","c"), Type.STRINGS);
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
        System.out.println(explain);
        String luceneQuery = explain.substring(0, explain.indexOf('\n'));
        assertEquals("[nt:unstructured] as [content] /* lucene:test1(/oak:index/test1) " +
                        "+(tags:Products:A tags:Products:A/B) " +
                        "+(tags:DocTypes:A tags:DocTypes:B tags:DocTypes:C tags:ProblemType:A)",
                luceneQuery);
    }

    @Test
    public void redundantNotNullCheck() throws Exception{
        Tree idx = createIndex("test1", of("tags"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("tags", of("a","b"), Type.STRINGS);
        test.addChild("b").setProperty("tags", of("a", "c"), Type.STRINGS);
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

    @Test
    public void propertyExistenceQuery2() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");

        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "propa");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "oak:TestNode").setProperty("propa", "a");
        createNodeWithType(test, "b", "oak:TestNode").setProperty("propa", "c");
        createNodeWithType(test, "c", "oak:TestNode").setProperty("propb", "e");
        root.commit();

        String propabQuery = "select [jcr:path] from [oak:TestNode] where [propa] is not null";
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1) :notNullProps:propa"));
        assertQuery(propabQuery, asList("/test/a", "/test/b"));
    }

    @Test
    public void propertyNonExistenceQuery() throws Exception {
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");

        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "propa");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "oak:TestNode").setProperty("propa", "a");
        createNodeWithType(test, "b", "oak:TestNode").setProperty("propa", "c");
        createNodeWithType(test, "c", "oak:TestNode").setProperty("propb", "e");
        root.commit();

        String propabQuery = "select [jcr:path] from [oak:TestNode] where [propa] is null";
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1) :nullProps:propa"));
        assertQuery(propabQuery, asList("/test/c"));
    }

    private static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    @Test
    public void orderByScore() throws Exception {
        Tree idx = createIndex("test1", of("propa"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa is not null order by [jcr:score]", asList("/test/a"));
    }

    @Test
    public void rangeQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
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

    @Test
    public void pathExclude() throws Exception{
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10", asList("/test/c"));

        //Make some change and then check
        test = root.getTree("/").getChild("test");
        test.addChild("a").addChild("e").setProperty("propa", 10);
        test.addChild("f").setProperty("propa", 10);
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10", asList("/test/c", "/test/f"));
    }

    //OAK-4516
    @Test
    public void wildcardQueryToLookupUnanalyzedText() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(PROP_TYPE, "lucene");
        idx.addChild(ANALYZERS).setProperty(INDEX_ORIGINAL_TERM, true);
        useV2(idx);
        //Do not provide type information
        root.commit();

        //setup propa def to be analyzed
        Tree propTree = root.getTree(idx.getPath() + "/indexRules/nt:base/properties/propa");
        propTree.setProperty(PROP_ANALYZED, true);
        root.commit();

        //set propb def to be node scope indexed
        propTree = root.getTree(idx.getPath() + "/indexRules/nt:base/properties/propb");
        propTree.setProperty(PROP_NODE_SCOPE_INDEX, true);
        root.getTree(idx.getPath()).setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();

        Tree rootTree = root.getTree("/");
        Tree node1Tree = rootTree.addChild("node1");
        node1Tree.setProperty("propa", "abcdef");
        node1Tree.setProperty("propb", "abcdef");
        Tree node2Tree = rootTree.addChild("node2");
        node2Tree.setProperty("propa", "abc_def");
        node2Tree.setProperty("propb", "abc_def");
        root.commit();

        //normal query still works
        String query = "select [jcr:path] from [nt:base] where contains('propa', 'abc*')";
        String explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/node1", "/node2"));

        //unanalyzed wild-card query can still match original term
        query = "select [jcr:path] from [nt:base] where contains('propa', 'abc_d*')";
        explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/node2"));

        //normal query still works
        query = "select [jcr:path] from [nt:base] where contains(*, 'abc*')";
        explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/node1", "/node2"));

        //unanalyzed wild-card query can still match original term
        query = "select [jcr:path] from [nt:base] where contains(*, 'abc_d*')";
        explanation = explain(query);
        assertThat(explanation, containsString("lucene:test1"));
        assertQuery(query, asList("/node2"));
    }

    //OAK-4517
    @Test
    public void pathIncludeSubrootIndex() throws Exception {
        Tree subTreeRoot = root.getTree("/").addChild("test");
        Tree idx = createIndex(subTreeRoot, "test1", of("propa"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/a"), Type.STRINGS));
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
        Tree idx = createIndex(subTreeRoot, "test1", of("propa"));
        idx.setProperty(createProperty(QUERY_PATHS, of("/test/a"), Type.STRINGS));
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
        Tree idx = createIndex(subTreeRoot, "test1", of("propa"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/a"), Type.STRINGS));
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
        Tree idx = createIndex("test1", of("propa", "propb"));
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
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
    public void likeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "humpty");
        test.addChild("b").setProperty("propa", "dumpty");
        test.addChild("c").setProperty("propa", "humpy");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa like 'hum%'",
                asList("/test/a", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ty'",
                asList("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ump%'",
            asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void nativeQueries() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        idx.setProperty(LuceneIndexConstants.FUNC_NAME, "foo");
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
        Tree idx = createIndex(parent, "test1", of("b/propa", "propb"));
        root.commit();

        Tree test = parent.addChild("test2");
        test.addChild("a").addChild("b").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] as s where [b/propa] = 'a'", asList("/test2/a"));

    }

    @Test
    public void indexDefinitionBelowRoot() throws Exception {
        Tree parent = root.getTree("/").addChild("test");
        Tree idx = createIndex(parent, "test1", of("propa", "propb"));
        idx.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
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
        Tree idx = createIndex(parent, "test1", of("propa", "propb"));
        idx.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
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
        Tree idx = createIndex(parent, "test1", of("propa"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        parent.setProperty("propa", "a");
        parent.addChild("test1").setProperty("propa", "a");
        root.commit();

        //asert that (1) result gets returned correctly, (2) parent isn't there, and (3) child is returned
        assertQuery("select [jcr:path] from [nt:base] as s where ISDESCENDANTNODE(s, '/test') and propa = 'a'", asList("/test/test1"));
    }

    @Test
    public void sortQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_NotIndexed() throws Exception {
        Tree idx = createIndex("test1", Collections.<String>emptySet());
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
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
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo/bar"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo").addChild("bar");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] order by [foo/bar]"), containsString("lucene:test1"));

        Tree test = root.getTree("/").addChild("test");
        List<Long> values = createLongs(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    @Test
    public void sortQueriesWithDouble_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    void assertSortedDouble() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Double> values = createDoubles(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    @Test
    public void sortQueriesWithString_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    @Test
    public void sortQueriesWithStringIgnoredMulti_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        List<String> values = createStrings(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n" + i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }

        //Add a wrong multi-valued property
        Tree child = test.addChild("a");
        child.setProperty("foo", of("w", "z"), Type.STRINGS);
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
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    @Test
    public void sortQueriesWithDate_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    void assertSortedDate() throws ParseException, CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
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
                Lists.newArrayList(Iterables.concat(Lists.newArrayList("/test/n0"),
                        getSortedPaths(tuples, OrderDirection.ASC))));
        // Append the path of property added as timestamp string to the sorted list
        assertOrderedQuery(
                "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", Lists
                        .newArrayList(Iterables.concat(getSortedPaths(tuples, OrderDirection.DESC),
                                Lists.newArrayList("/test/n0"))));
    }

    @Test
    public void sortQueriesWithStringAndLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar", "baz"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, ImmutableSet.of("foo", "baz"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("baz");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        int firstPropSize = 5;
        List<String> values = createStrings(firstPropSize);
        List<Long> longValues = createLongs(NUMBER_OF_NODES);
        List<Tuple2> tuples = Lists.newArrayListWithCapacity(values.size());
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
        Tree idx = createIndex("test1", of("propa", "propb", "propc"));
        idx.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);

        Tree propNode = idx.addChild(PROP_NODE);

        // property definition for index test1
        Tree propA = propNode.addChild("propa");
        propA.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_STRING);
        propA.setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);

        Tree propB = propNode.addChild("propb");
        propB.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_STRING);
        propB.setProperty(LuceneIndexConstants.FIELD_BOOST, 1.0);

        Tree propC = propNode.addChild("propc");
        propC.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_STRING);
        propC.setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);
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

        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);

        Tree title = props.addChild("title");
        title.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/jcr:title");
        title.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        title.setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);

        Tree desc = props.addChild("desc");
        desc.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/jcr:description");
        desc.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        desc.setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);

        Tree text = props.addChild("text");
        text.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/text");
        text.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);

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
        Tree idx = createIndex("test1", of("propa", "n0", "n1", "n2"));
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
        Tree idx = createIndex("test1", of("propa"));
        idx.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);
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
        List<? extends ResultRow> result = Lists.newArrayList(
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        TestUtil.useV2(idx);
        idx.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, true);

        Tree propNode = idx.addChild(PROP_NODE);

        // property definition for index test1
        Tree propA = propNode.addChild("propa");
        propA.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_STRING);
        propA.setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);

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
        Tree idx = createIndex("test1", of("propa", "propb", "propc"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("propc"), STRINGS));
        useV2(idx);

        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        List<Integer> nodes = Lists.newArrayList();
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
        Tree idx = createIndex("test1", of("propa", "propb", "propc", "propd"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("propd"), STRINGS));
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "original/jcr:content/type");
        prop1.setProperty(LuceneIndexConstants.PROP_EXCLUDE_FROM_AGGREGATE, true);
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, TestUtil.NT_TEST);
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "original/jcr:content/type");
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithMixinType(test, "a", "mix:title").setProperty("jcr:title", "a");
        createNodeWithMixinType(test, "b", "mix:title").setProperty("jcr:title", "c");
        test.addChild("c").setProperty("jcr:title", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:title] where [jcr:title] = 'a'";
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1)"));
        assertQuery(propabQuery, asList("/test/a"));
    }

    @Test
    public void indexingBasedOnMixinWithInheritence() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:mimeType");
        Tree prop = props.addChild(TestUtil.unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:mimeType");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        createNodeWithType(test, "a", "nt:resource").setProperty("jcr:mimeType", "a");
        createNodeWithType(test, "b", "nt:resource").setProperty("jcr:mimeType", "c");
        test.addChild("c").setProperty("jcr:mimeType", "a");
        root.commit();

        String propabQuery = "select [jcr:path] from [mix:mimeType] where [jcr:mimeType] = 'a'";
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1)"));
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
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:mimeType");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("jcr:mimeType", "1234");
        test.addChild("b").setProperty("other", "1234");
        test.addChild("c").setProperty("jcr:mimeType", "a");
        root.commit();

        String query;

        query = "/jcr:root/test//*[jcr:contains(@jcr:mimeType, '1234')]";
        assertThat(explainXpath(query), containsString("lucene:test2(/oak:index/test2)"));
        assertQuery(query, "xpath", asList("/test/a"));

        query = "/jcr:root/test//*[jcr:contains(., '1234')]";
        assertThat(explainXpath(query), containsString("no-index"));

        query = "/jcr:root/test//*[@jcr:mimeType = '1234']";
        assertThat(explainXpath(query), containsString("lucene:test2(/oak:index/test2)"));
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
        prop.setProperty(LuceneIndexConstants.PROP_NAME, "text");
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

        Tree prop2 = props.addChild(TestUtil.unique("prop"));
        prop2.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/type");
        prop2.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
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
        assertThat(explain(propabQuery), containsString("lucene:test1(/oak:index/test1)"));
        assertQuery(propabQuery, asList("/test/a"));
    }

    @Test
    public void reindexWithCOWWithoutIndexPath() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "mix:title");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        root.commit();

        //force CoR
        executeQuery("SELECT * FROM [mix:title]", SQL2);

        assertNotNull(corDir);
        String localPathBeforeReindex = corDir;

        //CoW with re-indexing
        idx.setProperty("reindex", true);
        root.commit();

        assertNotNull(cowDir);
        String localPathAfterReindex = cowDir;

        assertNotEquals("CoW should write to different dir on reindexing", localPathBeforeReindex, localPathAfterReindex);
    }

    @Test
    public void uniqueIdInitializedInIndexing() throws Exception{
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:title");
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
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
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "tag");
        prop1.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
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
        assertPlanAndQuery(propabQuery, "lucene:test1(/oak:index/test1)", asList("/test"));

        String query2 = "select * from [nt:base] where CONTAINS(tag, 'foo!')";
        assertPlanAndQuery(query2, "lucene:test1(/oak:index/test1)", asList("/test2"));

        String query3 = "select * from [nt:base] where CONTAINS(tag, 'a=b')";
        assertPlanAndQuery(query3, "lucene:test1(/oak:index/test1)", asList("/test3"));

        String query4 = "select * from [nt:base] where CONTAINS(tag, 'c=d=e')";
        assertPlanAndQuery(query4, "lucene:test1(/oak:index/test1)", asList("/test4"));

    }

    @Test
    public void fulltextQueryWithRelativeProperty() throws Exception{
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/metadata/comment");
        prop1.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("jcr:content").addChild("metadata").setProperty("comment", "taken in december");
        root.commit();

        String propabQuery = "select * from [nt:base] where CONTAINS([jcr:content/metadata/comment], 'december')";
        assertPlanAndQuery(propabQuery, "lucene:test1(/oak:index/test1)", asList("/test"));
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

    }

    @Test
    public void emptySuggestDictionary() throws Exception{
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree props = TestUtil.newRulePropTree(idx, "nt:base");
        Tree prop1 = props.addChild(TestUtil.unique("prop"));
        prop1.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop1.setProperty(LuceneIndexConstants.PROP_NAME, "tag");
        prop1.setProperty(LuceneIndexConstants.PROP_INDEX, true);
        prop1.setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
        root.commit();

        String query = "select * from [nt:base] where [tag] = 'foo'";
        assertPlanAndQuery(query, "lucene:test1(/oak:index/test1)", Collections.<String>emptyList());
    }

    @Test
    public void relativePropertyWithIndexOnNtBase() throws Exception {
        Tree idx = createIndex("test1", of("propa"));
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

        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
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

        assertPlanAndQuery("select * from [oak:TestSuperType]", "lucene:test1(/oak:index/test1)", asList("/a", "/b"));
        assertPlanAndQuery("select * from [oak:TestMixA]", "lucene:test1(/oak:index/test1)", asList("/b", "/c"));
    }

    @Test
    public void indexDefinitionModifiedPostReindex() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        String query = "select * from [nt:base] where [foo] = 'bar'";
        assertPlanAndQuery(query, "lucene:test1(/oak:index/test1)", asList("/a"));

        Tree barProp = root.getTree("/oak:index/test1/indexRules/nt:base/properties").addChild("bar");
        barProp.setProperty("name", "bar");
        barProp.setProperty("propertyIndex", true);
        root.commit();

        query = "select * from [nt:base] where [bar] = 'bar'";
        assertThat(explain(query), not(containsString("lucene:test1(/oak:index/test1)")));

        root.getTree("/oak:index/test1").setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();

        assertPlanAndQuery(query, "lucene:test1(/oak:index/test1)", asList("/b"));
    }

    @Test
    public void refreshIndexDefinition() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").setProperty("foo", "bar");
        rootTree.addChild("b").setProperty("bar", "bar");
        root.commit();

        String query = "select * from [nt:base] where [foo] = 'bar'";
        assertPlanAndQuery(query, "lucene:test1(/oak:index/test1)", asList("/a"));

        Tree barProp = root.getTree("/oak:index/test1/indexRules/nt:base/properties").addChild("bar");
        barProp.setProperty("name", "bar");
        barProp.setProperty("propertyIndex", true);
        root.commit();

        query = "select * from [nt:base] where [bar] = 'bar'";
        assertThat(explain(query), not(containsString("lucene:test1(/oak:index/test1)")));

        //Instead of reindex just refresh the index definition so that new index definition gets picked up
        root.getTree("/oak:index/test1").setProperty(LuceneIndexConstants.PROP_REFRESH_DEFN, true);
        root.commit();

        //Plan would reflect new defintion
        assertThat(explain(query), containsString("lucene:test1(/oak:index/test1)"));
        assertFalse(root.getTree("/oak:index/test1").hasProperty(LuceneIndexConstants.PROP_REFRESH_DEFN));

        //However as reindex was not done query would result in empty set
        assertPlanAndQuery(query, "lucene:test1(/oak:index/test1)", Collections.<String>emptyList());
    }

    @Test
    public void updateOldIndexDefinition() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
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

        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
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
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
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
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
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
                "lucene:test1(/oak:index/test1)", asList("/a", "/b"));

        assertPlanAndQuery("select * from [nt:base] where [jcr:content/metadata/sub/foo] = 'bar'",
                "lucene:test1(/oak:index/test1)", asList("/d"));
    }

    private void assertPlanAndQuery(String query, String planExpectation, List<String> paths){
        assertThat(explain(query), containsString(planExpectation));
        assertQuery(query, paths);
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
        prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        prop.setProperty(LuceneIndexConstants.FIELD_BOOST, boost);
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
        def.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(LuceneIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    private Tree createFullTextIndex(Tree index, String name) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree props = def.addChild(LuceneIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(LuceneIndexConstants.PROP_NODE)
                .addChild("allProps");

        props.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        props.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        props.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true);
        props.setProperty(LuceneIndexConstants.PROP_NAME, LuceneIndexConstants.REGEX_ALL_PROPS);
        props.setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
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
        List<String> paths = Lists.newArrayListWithCapacity(tuples.size());
        for (Tuple t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    static List<String> getSortedPaths(List<Tuple2> tuples) {
        Collections.sort(tuples);
        List<String> paths = Lists.newArrayListWithCapacity(tuples.size());
        for (Tuple2 t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    static List<Long> createLongs(int n){
        List<Long> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(i);
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Double> createDoubles(int n){
        Random rnd = new Random();
        List<Double> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(rnd.nextDouble());
        }
        Collections.shuffle(values);
        return values;
    }

    static List<String> createStrings(int n){
        List<String> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(String.format("value%04d",i));
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Calendar> createDates(int n) throws ParseException {
        Random rnd = new Random();
        List<Calendar> values = Lists.newArrayListWithCapacity(n);
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
            this(content.getBytes(Charsets.UTF_8));
        }

        public AccessStateProvidingBlob(String content, String id) {
            this(content.getBytes(Charsets.UTF_8));
            this.id = id;
        }

        @Nonnull
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
