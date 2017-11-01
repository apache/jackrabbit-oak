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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterConfig;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndexTest.createIndex;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newDoc;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiplexingLucenePropertyIndexTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState initialContent = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private MountInfoProvider mip = Mounts.newBuilder()
            .mount("foo", "/libs", "/apps").build();
    private NodeStore nodeStore;

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier = null;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier,
                new ExtractedTextCache(10*FileUtils.ONE_MB, 100),
                null,
                mip);
        LuceneIndexProvider provider = new LuceneIndexProvider(new IndexTracker(new DefaultIndexReaderFactory(mip, copier)));
        nodeStore = new MemoryNodeStore();
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Test
    public void numDocsIsSumOfAllReaders() throws Exception{
        NodeBuilder defnBuilder = newLucenePropertyIndexDefinition(builder, "test", ImmutableSet.of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(initialContent, defnBuilder.getNodeState(), "/foo");

        //1. Have 2 reader created by writes in 2 diff mounts
        DirectoryFactory directoryFactory = new DefaultDirectoryFactory(null, null);
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, directoryFactory, new LuceneIndexWriterConfig());
        LuceneIndexWriter writer = factory.newInstance(defn, builder, true);

        Document doc = newDoc("/content/en");
        doc.add(new StringField("foo", "bar", Field.Store.NO));
        writer.updateDocument("/content/en", doc);

        doc = newDoc("/libs/config");
        doc.add(new StringField("foo", "baz", Field.Store.NO));
        writer.updateDocument("/libs/config", doc);

        writer.close(0);

        //2. Construct the readers
        LuceneIndexReaderFactory readerFactory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = readerFactory.createReaders(defn, builder.getNodeState(),"/foo");

        IndexNodeManager node = new IndexNodeManager("foo", defn, readers, null);

        //3 Obtain the plan
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        IndexPlanner planner = new IndexPlanner(node.acquire(), "/foo", filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //Count should be sum of both readers
        assertEquals(2, plan.getEstimatedEntryCount());
    }

    @Test
    public void propertyIndex() throws Exception{
        String idxName = "multitest";
        createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        root.commit();

        createPath("/libs/a").setProperty("foo", "bar");
        createPath("/libs/b").setProperty("foo", "bar2");
        createPath("/content/a").setProperty("foo", "bar");
        root.commit();

        //There should be 2 index dir due to mount
        assertEquals(2, getIndexDirNames(idxName).size());

        String barQuery = "select [jcr:path] from [nt:base] where [foo] = 'bar'";
        assertQuery(barQuery, of("/libs/a", "/content/a"));

        Result result = executeQuery(barQuery, SQL2, NO_BINDINGS);
        assertTrue(result.getRows().iterator().hasNext());
        assertEquals(2, result.getSize(Result.SizePrecision.FAST_APPROXIMATION, 100));

        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar2'", of("/libs/b"));
    }

    @Test
    public void propertyIndexWithBatching() throws Exception{
        String idxName = "multitest";
        createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        root.commit();

        int expectedSize = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2 * 2;
        for (int i = 0; i < LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2; i++) {
            createPath("/libs/a"+i).setProperty("foo", "bar");
            createPath("/content/a"+i).setProperty("foo", "bar");
        }

        root.commit();
        assertEquals(2, getIndexDirNames(idxName).size());
        assertResultSize("select [jcr:path] from [nt:base] where [foo] = 'bar'", SQL2, expectedSize);
    }

    @Test
    public void sortQueriesWithStringAndLong() throws Exception {
        Tree idx = createIndex(root.getTree("/"), "test1", ImmutableSet.of("foo", "bar", "baz"));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, ImmutableSet.of("foo", "baz"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("baz");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        int firstPropSize = 25;
        List<String> values = LucenePropertyIndexTest.createStrings(firstPropSize);
        List<Long> longValues = LucenePropertyIndexTest.createLongs(LucenePropertyIndexTest.NUMBER_OF_NODES);
        List<LucenePropertyIndexTest.Tuple2> tuples = Lists.newArrayListWithCapacity(values.size());
        Random r = new Random();
        Tree libs = createPath("/libs");
        Tree content = createPath("/content");
        for(int i = 0; i < values.size(); i++){
            String val = values.get(r.nextInt(firstPropSize));
            Tree base = (i % 2 == 0 ? libs : content);
            Tree child = base.addChild("n"+i);
            child.setProperty("foo", val);
            child.setProperty("baz", longValues.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new LucenePropertyIndexTest.Tuple2(val, longValues.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] asc, [baz] desc",
                LucenePropertyIndexTest.getSortedPaths(tuples));
    }

    private List<String> getIndexDirNames(String indexName){
        NodeState idxDefn = NodeStateUtils.getNode(nodeStore.getRoot(), "/oak:index/"+indexName);
        List<String> names = new ArrayList<>();
        for (String childName : idxDefn.getChildNodeNames()){
            if (MultiplexersLucene.isIndexDirName(childName)){
                names.add(childName);
            }
        }
        return names;
    }

    private Tree createPath(String path){
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)){
            base = base.addChild(e);
        }
        return base;
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(initialContent);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private void assertOrderedQuery(String sql, List<String> paths) {
        assertOrderedQuery(sql, paths, SQL2, false);
    }

    private void assertOrderedQuery(String sql, List<String> paths, String language, boolean skipSort) {
        List<String> result = executeQuery(sql, language, true, skipSort);
        assertEquals(paths, result);
    }
}
