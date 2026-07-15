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
package org.apache.jackrabbit.oak.plugins.index.importer;

import org.apache.commons.io.IOUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexDefinitionUpdaterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final NodeStore store = new MemoryNodeStore();

    @Test
    public void update() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").child("fooIndex").setProperty("foo", "bar");
        builder.child("oak:index").child("fooIndex").child("a").setProperty("foo2", 2);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String json = createIndexDefn();
        applyJson(json);

        NodeState root = store.getRoot();

        assertTrue(NodeStateUtils.getNode(root, "/oak:index/fooIndex").exists());
        assertTrue(NodeStateUtils.getNode(root, "/oak:index/fooIndex/b").exists());
        assertFalse(NodeStateUtils.getNode(root, "/oak:index/fooIndex/a").exists());

        assertTrue(NodeStateUtils.getNode(root, "/oak:index/barIndex").exists());
    }

    @Test(expected = IllegalStateException.class)
    public void updateNonExistingParent() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").child("fooIndex").setProperty("foo", "bar");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder builder2 = EMPTY_NODE.builder();
        builder2.child("a").child("oak:index").child("barIndex").setProperty("foo", "bar");
        String json = createIndexDefn(builder2.getNodeState(), "/oak:index/fooIndex", "/a/oak:index/barIndex");
        applyJson(json);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidJson() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("a", Map.of("a2", "b2"));
        String json = JSONObject.toJSONString(map);
        applyJson(json);
    }

    @Test
    public void applyToIndexPath() throws Exception {
        String json = "{\"/oak:index/barIndex\": {\n" +
                "    \"compatVersion\": 2,\n" +
                "    \"type\": \"lucene\",\n" +
                "    \"barIndexProp\": \"barbar\",\n" +
                "    \"async\": \"async\",\n" +
                "    \"jcr:primaryType\": \"oak:QueryIndexDefinition\"\n" +
                "  }}";

        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinitionUpdater updater = new IndexDefinitionUpdater(json);
        builder = store.getRoot().builder();
        NodeBuilder idxBuilder = updater.apply(builder, "/oak:index/barIndex");

        //Check builder returned is of /oak:index/barIndex
        assertTrue(idxBuilder.hasProperty("barIndexProp"));
    }

    @Test
    public void newIndexAndOrderableChildren() throws Exception {
        String json = "{\"/oak:index/barIndex\": {\n" +
                "    \"compatVersion\": 2,\n" +
                "    \"type\": \"lucene\",\n" +
                "    \"barIndexProp\": \"barbar\",\n" +
                "    \"async\": \"async\",\n" +
                "    \"jcr:primaryType\": \"oak:QueryIndexDefinition\"\n" +
                "  }}";

        NodeBuilder builder = store.getRoot().builder();
        Tree root = TreeFactory.createTree(builder);
        Tree oakIndex = root.addChild("oak:index");
        oakIndex.setOrderableChildren(true);
        Tree fooIndex = oakIndex.addChild("fooIndex");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinitionUpdater updater = new IndexDefinitionUpdater(json);
        builder = store.getRoot().builder();
        NodeBuilder idxBuilder = updater.apply(builder, "/oak:index/barIndex");

        PropertyState childOrder = builder.getChildNode("oak:index").getProperty(":childOrder");
        List<String> names = ListUtils.toList(childOrder.getValue(Type.NAMES));

        assertEquals(asList("fooIndex", "barIndex"), names);

    }


    @Test
    public void fixJCRUUID() throws Exception {
        String json;
        try (InputStream is = getClass().getResourceAsStream("/org/apache/jackrabbit/oak/plugins/index/importer/index-def-jcruuid-fix.json")) {
            json = IOUtils.toString(is, StandardCharsets.UTF_8);
        }

        NodeBuilder builder = store.getRoot().builder();
        Tree root = TreeFactory.createTree(builder);
        Tree oakIndex = root.addChild("oak:index");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinitionUpdater updater = new IndexDefinitionUpdater(json);
        builder = store.getRoot().builder();

        NodeBuilder idxBuilder = updater.apply(builder, "/oak:index/barIndex");

        NodeBuilder barIndexBuilder = builder
                .getChildNode("oak:index")
                .getChildNode("barIndex");
        // The node config_without_uuid.xml does not contain any jcr:uuid, so we only check that now there is one
        checkContainsJcrUUID(barIndexBuilder
                .getChildNode("config_without_uuid.xml")
                .getChildNode("jcr:content"));

        // This node config_with_uuid.xml contains a jcr:uuid, so check that the new jcr:uuid is different
        UUID newUuid = checkContainsJcrUUID(barIndexBuilder
                .getChildNode("config_with_uuid.xml")
                .getChildNode("jcr:content"));
        assertFalse(newUuid.toString().equalsIgnoreCase("6db23ff7-986d-4c3d-bf3d-ea9eaf8b5f65"));

    }

    private UUID checkContainsJcrUUID(NodeBuilder builder) {
        @Nullable PropertyState jcrUuid = builder.getProperty(JcrConstants.JCR_UUID);
        assertNotNull(jcrUuid);
        String newUuid = jcrUuid.getValue(Type.STRING);
        assertNotNull(newUuid);
        return UUID.fromString(newUuid);
    }

    private void applyJson(String json) throws IOException, CommitFailedException {
        IndexDefinitionUpdater update = new IndexDefinitionUpdater(json);

        NodeBuilder builder = store.getRoot().builder();
        update.apply(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private String createIndexDefn() throws CommitFailedException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("oak:index").child("fooIndex").setProperty("foo", "bar");
        builder.child("oak:index").child("fooIndex").setProperty("foo3", "bar3");
        builder.child("oak:index").child("fooIndex").child("b").setProperty("foo4", 4);

        builder.child("oak:index").child("barIndex").setProperty("foo", "bar");
        builder.child("oak:index").child("barIndex").child("c").setProperty("foo5", 5);
        return createIndexDefn(builder.getNodeState(), "/oak:index/fooIndex", "/oak:index/barIndex");
    }

    private String createIndexDefn(NodeState nodeState, String... indexPaths) throws CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        for (ChildNodeEntry cne : nodeState.getChildNodeEntries()) {
            builder.setChildNode(cne.getName(), cne.getNodeState());
        }

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinitionPrinter printer = new IndexDefinitionPrinter(store, () -> asList(indexPaths));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printer.print(pw, Format.JSON, false);

        pw.flush();

        return sw.toString();
    }

}