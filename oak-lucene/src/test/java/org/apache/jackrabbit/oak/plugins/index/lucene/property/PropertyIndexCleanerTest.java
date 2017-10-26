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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyIndexCleaner.CleanupStats;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PropertyIndexCleanerTest {
    private NodeStore nodeStore = new MemoryNodeStore();
    private SimpleAsyncInfoService asyncService = new SimpleAsyncInfoService();
    private Clock clock = new Clock.Virtual();

    @Before
    public void setUp() throws CommitFailedException {
        NodeBuilder nb = nodeStore.getRoot().builder();
        new InitialContent().initialize(nb);
        merge(nb);
    }

    @Test
    public void syncIndexPaths() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        String indexPath = "/oak:index/foo";
        addIndex(indexPath, defnb);

        PropertyIndexCleaner cleaner =
                new PropertyIndexCleaner(nodeStore, () -> asList("/oak:index/uuid", indexPath), asyncService, StatisticsProvider.NOOP);

        //As index is yet not update it would not show up in sync index paths
        assertThat(cleaner.getSyncIndexPaths(), empty());

        NodeBuilder builder = nodeStore.getRoot().builder();
        PropertyIndexUpdateCallback cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/a", "foo", "bar");
        merge(builder);

        //Post update it would show up
        assertThat(cleaner.getSyncIndexPaths(), containsInAnyOrder(indexPath));
    }

    @Test
    public void simplePropertyIndexCleaning() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        String indexPath = "/oak:index/foo";
        addIndex(indexPath, defnb);

        PropertyIndexCleaner cleaner =
                new PropertyIndexCleaner(nodeStore, () -> asList("/oak:index/uuid", indexPath), asyncService, StatisticsProvider.NOOP);

        NodeBuilder builder = nodeStore.getRoot().builder();
        PropertyIndexUpdateCallback cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/a", "foo", "bar");
        merge(builder);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a"));

        //------------------------ Run 1
        asyncService.addInfo("async", 1000);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a"));

        builder = nodeStore.getRoot().builder();
        cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/b", "foo", "bar");
        merge(builder);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a", "/b"));

        //------------------------ Run 2
        asyncService.addInfo("async", 2000);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);

        //Now /a would be part of removed bucket
        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/b"));

        //------------------------ Run 3
        asyncService.addInfo("async", 3000);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);

        //With another run /b would also be removed
        assertThat(query(indexPath, "foo", "bar"), empty());
    }

    @Test
    public void uniqueIndexCleaning() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();
        String indexPath = "/oak:index/foo";
        addIndex(indexPath, defnb);

        PropertyIndexCleaner cleaner =
                new PropertyIndexCleaner(nodeStore, () -> asList("/oak:index/uuid", indexPath), asyncService, StatisticsProvider.NOOP);
        cleaner.setCreatedTimeThreshold(TimeUnit.MILLISECONDS, 100);

        clock.waitUntil(1000);

        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child("a").setProperty("foo", "bar");
        PropertyIndexUpdateCallback cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/a", "foo", "bar");
        cb.done();
        merge(builder);

        clock.waitUntil(1150);

        builder = nodeStore.getRoot().builder();
        builder.child("b").setProperty("foo", "bar2");
        cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/b", "foo", "bar2");
        cb.done();
        merge(builder);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a"));
        assertThat(query(indexPath, "foo", "bar2"), containsInAnyOrder("/b"));

        //------------------------ Run 1
        asyncService.addInfo("async", 1200);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);
        assertJsonInfo(indexPath, "{\n" +
                "  \"foo\": {\n" +
                "    \"entryCount\": 1,\n" +
                "    \"unique\": true\n" +
                "  }\n" +
                "}");

        // /a would be purged, /b would be retained as its created time 1150 is not older than 100 wrt
        // indexer time of 1200
        assertThat(query(indexPath, "foo", "bar"), empty());
        assertThat(query(indexPath, "foo", "bar2"), containsInAnyOrder("/b"));

        builder = nodeStore.getRoot().builder();
        builder.child("c").setProperty("foo", "bar2");
        cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/c", "foo", "bar2");

        try{
            cb.done();
            fail();
        } catch (CommitFailedException e){
            assertEquals(CONSTRAINT,e.getType());
        }

        //------------------------ Run 2
        asyncService.addInfo("async", 1400);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);

        //Both entries would have been purged
        assertThat(query(indexPath, "foo", "bar"), empty());
        assertThat(query(indexPath, "foo", "bar2"), empty());
    }

    @Test
    public void noRunPerformedIfNoChangeInAsync() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        String indexPath = "/oak:index/foo";
        addIndex(indexPath, defnb);

        PropertyIndexCleaner cleaner =
                new PropertyIndexCleaner(nodeStore, () -> asList("/oak:index/uuid", indexPath), asyncService, StatisticsProvider.NOOP);

        NodeBuilder builder = nodeStore.getRoot().builder();
        PropertyIndexUpdateCallback cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/a", "foo", "bar");
        merge(builder);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a"));

        //------------------------ Run 1
        asyncService.addInfo("async", 1000);
        assertCleanUpPerformed(cleaner.performCleanup(false), true);
        assertJsonInfo(indexPath, "{\n" +
                "  \"foo\": {\n" +
                "    \"1\": {\n" +
                "      \"type\": \"previous\",\n" +
                "      \"keyCount\": 1,\n" +
                "      \"entryCount\": 1,\n" +
                "      \"totalCount\": 3\n" +
                "    },\n" +
                "    \"2\": {\n" +
                "      \"type\": \"head\",\n" +
                "      \"keyCount\": 0,\n" +
                "      \"entryCount\": 0,\n" +
                "      \"totalCount\": 1\n" +
                "    }\n" +
                "  }\n" +
                "}");

        //Second run should not run
        assertCleanUpPerformed(cleaner.performCleanup(false), false);
    }

    @Test
    public void recursiveDelete() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        String indexPath = "/oak:index/foo";
        addIndex(indexPath, defnb);

        PropertyIndexCleaner cleaner =
                new PropertyIndexCleaner(nodeStore, () -> asList("/oak:index/uuid", indexPath), asyncService, StatisticsProvider.NOOP);
        cleaner.setRecursiveDelete(true);

        NodeBuilder builder = nodeStore.getRoot().builder();
        PropertyIndexUpdateCallback cb = newCallback(builder, indexPath);
        propertyUpdated(cb, indexPath, "/a", "foo", "bar");
        merge(builder);

        assertThat(query(indexPath, "foo", "bar"), containsInAnyOrder("/a"));

        //------------------------ Run 1
        asyncService.addInfo("async", 1000);
        CleanupStats stats = cleaner.performCleanup(false);
        assertCleanUpPerformed(stats, true);

        asyncService.addInfo("async", 2000);
        stats = cleaner.performCleanup(false);

        //1 - for bucket
        //1 - for indexed value 'bar'
        //1 - for indexed path 'a'
        assertEquals(3, stats.numOfNodesDeleted);

    }

    private void assertCleanUpPerformed(CleanupStats stats, boolean expected) {
        assertEquals(expected, stats.cleanupPerformed);
    }

    private void assertJsonInfo(String indexPath, String expectedJson) throws ParseException {
        NodeState idx = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);
        String json = new HybridPropertyIndexInfo(idx).getInfoAsJson();
        JsonObject j1 = (JsonObject) new JsonParser().parse(json);
        JsonObject j2 = (JsonObject) new JsonParser().parse(expectedJson);

        if (!j1.equals(j2)){
            assertEquals(j1, j2);
        }
    }

    private void addIndex(String indexPath, IndexDefinitionBuilder defnb) throws CommitFailedException {
        NodeBuilder nb = nodeStore.getRoot().builder();
        child(nb, getParentPath(indexPath)).setChildNode(getName(indexPath), defnb.build());
        merge(nb);
    }

    private void propertyUpdated(PropertyUpdateCallback callback, String indexPath, String nodePath, String propertyName,
                                 String value){
        callback.propertyUpdated(nodePath, propertyName, pd(indexPath, propertyName),
                null, createProperty(propertyName, value));
    }

    private PropertyIndexUpdateCallback newCallback(NodeBuilder builder, String indexPath) {
        return new PropertyIndexUpdateCallback(indexPath, child(builder, indexPath), builder.getNodeState(), clock);
    }

    private PropertyDefinition pd(String indexPath, String propName){
        NodeState root = nodeStore.getRoot();
        IndexDefinition defn = new IndexDefinition(root, getNode(root, indexPath), indexPath);
        return defn.getApplicableIndexingRule("nt:base").getConfig(propName);
    }

    private void merge(NodeBuilder nb) throws CommitFailedException {
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private List<String> query(String indexPath, String propertyName, String value) {
        NodeState root = nodeStore.getRoot();
        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(indexPath, getNode(root, indexPath));
        FilterImpl filter = FilterImpl.newTestInstance();
        Iterable<String> paths = lookup.query(filter, propertyName,   PropertyValues.newString(value));
        return ImmutableList.copyOf(paths);
    }

    private static class SimpleAsyncInfoService implements AsyncIndexInfoService {
        final Map<String, AsyncIndexInfo> infos = new HashMap<>();

        @Override
        public Iterable<String> getAsyncLanes() {
            return infos.keySet();
        }

        @Override
        public Iterable<String> getAsyncLanes(NodeState root) {
            throw new UnsupportedOperationException();
        }

        @CheckForNull
        @Override
        public AsyncIndexInfo getInfo(String name) {
            return infos.get(name);
        }

        @CheckForNull
        @Override
        public AsyncIndexInfo getInfo(String name, NodeState root) {
            return null;
        }

        @Override
        public Map<String, Long> getIndexedUptoPerLane() {
            Map<String, Long> result = new HashMap<>();
            for (AsyncIndexInfo info : infos.values()) {
                result.put(info.getName(), info.getLastIndexedTo());
            }
            return result;
        }

        @Override
        public Map<String, Long> getIndexedUptoPerLane(NodeState root) {
            throw new UnsupportedOperationException();
        }

        public void addInfo(String name, long lastIndexedTo) {
            infos.put(name, new AsyncIndexInfo(name, lastIndexedTo, 0, false, null));
        }
    }


}