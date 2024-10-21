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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.run.GenerateGarbageCommand;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.document.CommandTestUtils.captureSystemOut;
import static org.apache.jackrabbit.oak.run.GenerateGarbageCommand.EMPTY_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class GenerateGarbageCommandTest {

    final String OPTION_GARBAGE_NODES_COUNT = "--garbageNodesCount";
    final String OPTION_GARBAGE_NODES_PARENT_COUNT = "--garbageNodesParentCount";
    final String OPTION_GARBAGE_TYPE = "--garbageType";
    final String OPTION_NUMBER_OF_RUNS = "--numberOfRuns";
    final String OPTION_GENERATE_INTERVAL_SECONDS = "--generateIntervalSeconds";

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    private Closer closer;

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() {
        ns = createDocumentNodeStore();
    }

    @After
    public void after() {
        try {
            closer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ns.dispose();
    }

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setFullGCEnabled(false).setLeaseCheckMode(LeaseCheckMode.DISABLED).setAsyncDelay(0)
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    private List<String> getChildNodeNames(NodeState node) {
        List<String> childNodeNames = new ArrayList<>();
        for (String childNodeName : node.getChildNodeNames()) {
            childNodeNames.add(childNodeName);
        }
        return childNodeNames;
    }

    @Ignore
    @Test
    public void generateGarbageEmptyPropsUnderOneParentOnCustomMongoDB() {
        ns.dispose();

        // this should be a valid mongoDB connection string for the MongoDB on which to generate the garbage
        String mongoDBConnString = "";
        GenerateFullGCCmd cmd = new GenerateFullGCCmd( mongoDBConnString, OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "1");
        String output = captureSystemOut(cmd);

        //sleep thread for 10 seconds to allow the garbage generation to complete
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getFullGCRootNode(nodeStore);
        List<String> garbageRootNodeChildNames = getChildNodeNames(garbageRootNodeState);

        assertEquals(garbageRootNodeChildNames.size(), 1);
        List<String> propParentsGarbageNodeNames = getChildNodeNames(garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)));
        assertEquals(propParentsGarbageNodeNames.size(), 1);

        List<String> propGarbageNodeNames = getChildNodeNames(garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)).getChildNode(propParentsGarbageNodeNames.get(0)));
        assertEquals(propGarbageNodeNames.size(), 10);
    }

    @Test
    public void cleanupAllGarbage() {

        // generate garbage first
        GenerateFullGCCmd cmdGenerateGarbage = new GenerateFullGCCmd(OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "2");
        cmdGenerateGarbage.run();
        Closer cmd1Closer = cmdGenerateGarbage.getCloser();

        // cleanup garbage
        GenerateFullGCCmd cmdClean = new GenerateFullGCCmd("clean");
        cmdClean.run();
        closer = cmdClean.getCloser();

        NodeDocument garbageRoot = getDocument(cmdClean, Collection.NODES, Utils.getIdFromPath("/" + GenerateGarbageCommand.FULLGC_GEN_ROOT_PATH), 0);
        // garbage root node should be either deleted or empty (no children)
        assertTrue(garbageRoot == null || !garbageRoot.hasChildren());

        try {
            cmd1Closer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void generateGarbageEmptyPropsUnderOneParent() {
        ns.dispose();

        GenerateFullGCCmd cmd = new GenerateFullGCCmd(OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "1");
        String output = captureSystemOut(cmd);
        closer = cmd.getCloser();

        List<String> generatedBasePaths = cmd.getGeneratedBasePaths();

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getFullGCRootNode(nodeStore);
        List<String> garbageRootNodeChildNames = getChildNodeNames(garbageRootNodeState);

        assertEquals(garbageRootNodeChildNames.size(), 1);
        for (String generateBasePath : generatedBasePaths) {
            assertTrue(garbageRootNodeChildNames.contains(generateBasePath));
        }

        List<String> propParentsGarbageNodeNames = getChildNodeNames(garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)));
        assertEquals(propParentsGarbageNodeNames.size(), 1);

        NodeState emptyPropsParentNode = garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)).getChildNode(propParentsGarbageNodeNames.get(0));
        List<String> propGarbageNodeNames = getChildNodeNames(emptyPropsParentNode);
        assertEquals(propGarbageNodeNames.size(), 10);

        for (String propGarbageNodeName : propGarbageNodeNames) {
            NodeState propGarbageNode = emptyPropsParentNode.getChildNode(propGarbageNodeName);
            assertNull(propGarbageNode.getProperty(EMPTY_PROPERTY_NAME));
        }
    }

    @Test
    public void generateGarbageGapOrphansUnderOneParent() {
        ns.dispose();

        int garbageNodesCount = 10;
        int garbageNodesParentCount = 1;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount);
    }

    @Test
    public void generateGarbageGapOrphansUnderMultipleParents() {
        ns.dispose();

        int garbageNodesCount = 20;
        int garbageNodesParentCount = 4;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount);
    }

    /**
     * Calls the GenerateFullGCCommand to generate gap orphans and verifies the generated gap orphans:
     * - checks that parents of gap orphans are missing by retrieving the nodes from the document store
     * - checks that the expected number gap orphans are generated by retrieving the nodes from the document store
     * @param garbageNodesCount
     * @param garbageNodesParentCount
     */
    private void testGenerateGapOrphans(int garbageNodesCount, int garbageNodesParentCount) {
        GenerateFullGCCmd cmd = new GenerateFullGCCmd(OPTION_GARBAGE_NODES_COUNT, String.valueOf(garbageNodesCount),
                OPTION_GARBAGE_NODES_PARENT_COUNT, String.valueOf(garbageNodesParentCount),
                OPTION_GARBAGE_TYPE, "2");
        String output = captureSystemOut(cmd);
        closer = cmd.getCloser();

        int nodesPerParent = garbageNodesCount / garbageNodesParentCount;

        List<String> generatedBasePaths = cmd.getGeneratedBasePaths();

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getFullGCRootNode(nodeStore);
        List<String> garbageRootNodeChildNames = getChildNodeNames(garbageRootNodeState);

        assertEquals(garbageRootNodeChildNames.size(), 1);
        for (String generateBasePath : generatedBasePaths) {
            assertTrue(garbageRootNodeChildNames.contains(generateBasePath));
        }

        String generateBasePath = generatedBasePaths.get(0);
        for (int i = 0; i < garbageNodesParentCount; i ++) {
            NodeDocument missingDocument = getDocument(cmd, Collection.NODES, Utils.getIdFromPath(
                    "/" + GenerateGarbageCommand.FULLGC_GEN_ROOT_PATH + "/" + generateBasePath + "/"
                    + GenerateGarbageCommand.GEN_PARENT_NODE_PREFIX + i), 0);

            assertNull(missingDocument);

            for(int j = 0; j < nodesPerParent; j++) {
                NodeDocument gapOrphanDocument = getDocument(cmd, Collection.NODES, Utils.getIdFromPath(
                        "/" + GenerateGarbageCommand.FULLGC_GEN_ROOT_PATH + "/" + generateBasePath + "/"
                        + GenerateGarbageCommand.GEN_PARENT_NODE_PREFIX + i + "/"
                        + GenerateGarbageCommand.GEN_NODE_PREFIX + j), 0);
                assertNotNull(gapOrphanDocument);
            }
        }
    }

    /**
     * Utility method that returns the root node of the full gc generated garbage nodes.
     * @param nodeStore
     * @return
     */
    private static @NotNull NodeState getFullGCRootNode(DocumentNodeStore nodeStore) {
        NodeState garbageRootNodeState = nodeStore.getRoot().getChildNode(GenerateGarbageCommand.FULLGC_GEN_ROOT_PATH_BASE).
                getChildNode(GenerateGarbageCommand.FULLGC_GEN_ROOT_NODE_NAME);
        return garbageRootNodeState;
    }

    /**
     * Used in unit tests for retrieving a document by path from the document store using the documentNodeStore
     * of this class.
     * @param collection
     * @param key
     * @param maxCacheAge
     * @return
     * @param <T>
     * @throws DocumentStoreException
     */
    @Nullable
    private  <T extends Document> T getDocument(GenerateFullGCCmd cmd, Collection<T> collection, String key, int maxCacheAge)
            throws DocumentStoreException {
        return cmd.getCommand().getDocumentNodeStore().getDocumentStore().find(collection, key, maxCacheAge);
    }

    private static class GenerateFullGCCmd implements Runnable {

        private final ImmutableList<String> args;
        private GenerateGarbageCommand command;
        private Closer closer;
        private List<String> generatedBasePaths;

        public GenerateFullGCCmd(String... args) {
            // append the default mongodb connection string if one was not provided
            if (args[0].startsWith("mongodb://")) {
                this.args = ImmutableList.copyOf(args);
            } else {
                this.args = ImmutableList.<String>builder().add(MongoUtils.URL)
                        .add(args).build();
            }
        }

        @Override
        public void run() {
            try {
                closer = Closer.create();
                command = new GenerateGarbageCommand();
                generatedBasePaths = command.execute(closer, args.toArray(new String[0]));
            } catch (Throwable e) {
                String str = e.getMessage();
            }
        }

        public Closer getCloser() {
            return closer;
        }

        public List<String> getGeneratedBasePaths() {
            return generatedBasePaths;
        }

        public GenerateGarbageCommand getCommand() {
            return command;
        }
    }
}
