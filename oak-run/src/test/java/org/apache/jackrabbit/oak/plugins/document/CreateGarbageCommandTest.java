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

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.run.CreateGarbageCommand;
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
import java.util.Arrays;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.document.CommandTestUtils.captureSystemOut;
import static org.apache.jackrabbit.oak.run.CreateGarbageCommand.EMPTY_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class CreateGarbageCommandTest {

    final String OPTION_GARBAGE_NODES_COUNT = "--garbageNodesCount";
    final String OPTION_GARBAGE_NODES_PARENT_COUNT = "--garbageNodesParentCount";
    final String OPTION_GARBAGE_TYPE = "--garbageType";
    final String OPTION_GARBAGE_ORPHANS_DEPTH = "--orphansDepth";
    final String OPTION_GARBAGE_ORPHANS_LEVEL_GAP = "--orphansLevelGap";
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

    /**
     * This test generates garbage empty properties under one parent node and then cleans up the garbage.
     * Use a custom MongoDB connection string for the MongoDB to run the test on.
     * Long running test, should only be run manually.
     */
    @Ignore
    @Test
    public void generateGarbageEmptyPropsUnderOneParentOnCustomMongoDB() {
        ns.dispose();

        // this should be a valid mongoDB connection string for the MongoDB on which to generate the garbage
        String mongoDBConnString = "";
        CreateGarbageCmd cmd = new CreateGarbageCmd( mongoDBConnString, OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "1");
        String output = captureSystemOut(cmd);

        //sleep thread for 10 seconds to allow the garbage generation to complete
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getGarbageRootNode(nodeStore);
        List<String> garbageRootNodeChildNames = getChildNodeNames(garbageRootNodeState);

        assertEquals(garbageRootNodeChildNames.size(), 1);
        List<String> propParentsGarbageNodeNames = getChildNodeNames(garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)));
        assertEquals(propParentsGarbageNodeNames.size(), 1);

        List<String> propGarbageNodeNames = getChildNodeNames(garbageRootNodeState.getChildNode(garbageRootNodeChildNames.get(0)).getChildNode(propParentsGarbageNodeNames.get(0)));
        assertEquals(propGarbageNodeNames.size(), 10);
    }

    /**
     * This test generates garbage empty properties and then cleans up the garbage.
     * Long running test, should only be run manually.
     */
    @Ignore
    @Test
    public void cleanupAllGarbageEmptyProperties() {

        // generate garbage first
        CreateGarbageCmd cmdGenerateGarbage = new CreateGarbageCmd(OPTION_GARBAGE_NODES_COUNT, "4", OPTION_GARBAGE_NODES_PARENT_COUNT, "3",
                OPTION_GARBAGE_TYPE, "1");
        cmdGenerateGarbage.run();
        Closer cmd1Closer = cmdGenerateGarbage.getCloser();

        // cleanup garbage
        CreateGarbageCmd cmdClean = new CreateGarbageCmd("clean");
        cmdClean.run();
        closer = cmdClean.getCloser();

        NodeDocument garbageRoot = getDocument(cmdClean, Collection.NODES, Utils.getIdFromPath("/" + CreateGarbageCommand.GARBAGE_GEN_ROOT_PATH), 0);
        // garbage root node should be either deleted or empty (no children)
        assertTrue(garbageRoot == null || !garbageRoot.hasChildren());

        try {
            cmd1Closer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This test generates garbage gap orphans and then cleans up the garbage.
     * Long running test, should only be run manually.
     */
    @Ignore
    @Test
    public void cleanupAllGarbageGapOrphans() {

        // generate garbage first
        CreateGarbageCmd cmdGenerateGarbage = new CreateGarbageCmd(OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "2");
        cmdGenerateGarbage.run();
        Closer cmd1Closer = cmdGenerateGarbage.getCloser();

        // cleanup garbage
        CreateGarbageCmd cmdClean = new CreateGarbageCmd("clean");
        cmdClean.run();
        closer = cmdClean.getCloser();

        NodeDocument garbageRoot = getDocument(cmdClean, Collection.NODES, Utils.getIdFromPath("/" + CreateGarbageCommand.GARBAGE_GEN_ROOT_PATH), 0);
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

        CreateGarbageCmd cmd = new CreateGarbageCmd(OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "1");
        String output = captureSystemOut(cmd);
        closer = cmd.getCloser();

        List<String> generatedBasePaths = cmd.getGeneratedBasePaths();

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getGarbageRootNode(nodeStore);
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

    @Test
    public void generateGarbageGapOrphansWithDepthAndMinLevelGap() {
        ns.dispose();

        int garbageNodesCount = 5;
        int garbageNodesParentCount = 1;
        int orphansDepth = 7;
        int orphansLevelGap = 0;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount, orphansDepth, orphansLevelGap);
    }

    @Test
    public void generateGarbageGapOrphansWithDepthAndMaxLevelMinusOneGap() {
        ns.dispose();

        int garbageNodesCount = 1;
        int garbageNodesParentCount = 1;
        int orphansDepth = 10;
        int orphansLevelGap = 9;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount, orphansDepth, orphansLevelGap);
    }

    @Test
    public void generateGarbageGapOrphansWithDepthAndMaxLevelGap() {
        ns.dispose();

        int garbageNodesCount = 5;
        int garbageNodesParentCount = 1;
        int orphansDepth = 11;
        int orphansLevelGap = 11;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount, orphansDepth, orphansLevelGap);
    }

    @Test
    public void generateGarbageGapOrphansWithDepthAndMiddleLevelGap() {
        ns.dispose();

        int garbageNodesCount = 5;
        int garbageNodesParentCount = 1;
        int orphansDepth = 8;
        int orphansLevelGap = 4;

        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount, orphansDepth, orphansLevelGap);
    }

    /**
     * Calls the GenerateFullGCCommand to generate gap orphans and verifies the generated gap orphans were created
     * correctly.
     * Gap orphan depth is minimum and level gap is 0.
     * @param garbageNodesCount
     * @param garbageNodesParentCount
     */
    private void testGenerateGapOrphans(int garbageNodesCount, int garbageNodesParentCount) {
        testGenerateGapOrphans(garbageNodesCount, garbageNodesParentCount, 1, 0);
    }

    /**
     * Calls the GenerateFullGCCommand to generate gap orphans and verifies the generated gap orphans:
     * - checks that parents of gap orphans are missing by retrieving the nodes from the document store
     * - checks that the expected number gap orphans are generated by retrieving the nodes from the document store
     * @param garbageNodesCount
     * @param garbageNodesParentCount
     * @param orphansDepth - the depth of the leaf garbage gap orphan node
     * @param orphansLevelGap - the level that is used as threshold for deletion - all nodes from the parent to this node
     *                        will be deleted
     */
    private void testGenerateGapOrphans(int garbageNodesCount, int garbageNodesParentCount,
                                        int orphansDepth, int orphansLevelGap) {
        CreateGarbageCmd cmd = new CreateGarbageCmd(OPTION_GARBAGE_NODES_COUNT, String.valueOf(garbageNodesCount),
                OPTION_GARBAGE_NODES_PARENT_COUNT, String.valueOf(garbageNodesParentCount),
                OPTION_GARBAGE_ORPHANS_DEPTH, String.valueOf(orphansDepth),
                OPTION_GARBAGE_ORPHANS_LEVEL_GAP, String.valueOf(orphansLevelGap),
                OPTION_GARBAGE_TYPE, "2");
        String output = captureSystemOut(cmd);
        closer = cmd.getCloser();

        int nodesPerParent = garbageNodesCount / garbageNodesParentCount;

        List<String> generatedBasePaths = cmd.getGeneratedBasePaths();

        DocumentNodeStore nodeStore = cmd.getCommand().getDocumentNodeStore();
        NodeState garbageRootNodeState = getGarbageRootNode(nodeStore);
        List<String> garbageRootNodeChildNames = getChildNodeNames(garbageRootNodeState);

        assertEquals(garbageRootNodeChildNames.size(), 1);
        for (String generateBasePath : generatedBasePaths) {
            assertTrue(garbageRootNodeChildNames.contains(generateBasePath));
        }

        String generateBasePath = generatedBasePaths.get(0);
        for (int i = 0; i < garbageNodesParentCount; i ++) {

            for (int j = 0; j < nodesPerParent; j++) {

                // check deleted node paths
                List<String> deletedNodePaths = CreateGarbageCommand.generateGapOrphanNodePaths(
                        generateBasePath, i, j, orphansDepth, orphansLevelGap, false);
                for (String deletedNodePath : deletedNodePaths) {
                    NodeDocument missingDocument = getDocument(cmd, Collection.NODES, deletedNodePath, 0);
                    assertNull(missingDocument);
                }

                // check not deleted node paths - remaining gap orphans
                List<String> notDeletedNodePaths = CreateGarbageCommand.generateGapOrphanNodePaths(generateBasePath, i, j, orphansDepth, orphansLevelGap, true);
                for (String notDeletedNodePath : notDeletedNodePaths) {
                    NodeDocument gapOrphanDocument = getDocument(cmd, Collection.NODES, notDeletedNodePath, 0);
                    assertNotNull(gapOrphanDocument);
                }
            }
        }
    }

    /**
     * Utility method that returns the root node of the full gc generated garbage nodes.
     * @param nodeStore
     * @return
     */
    private static @NotNull NodeState getGarbageRootNode(DocumentNodeStore nodeStore) {
        NodeState garbageRootNodeState = nodeStore.getRoot().getChildNode(CreateGarbageCommand.GARBAGE_GEN_ROOT_PATH_BASE).
                getChildNode(CreateGarbageCommand.GARBAGE_GEN_ROOT_NODE_NAME);
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
    private  <T extends Document> T getDocument(CreateGarbageCmd cmd, Collection<T> collection, String key, int maxCacheAge)
            throws DocumentStoreException {
        return cmd.getCommand().getDocumentNodeStore().getDocumentStore().find(collection, key, maxCacheAge);
    }

    /**
     * Wrapper class for the GenerateGarbageCommand to be used in unit tests.
     */
    private static class CreateGarbageCmd implements Runnable {

        private final List<String> args;
        private CreateGarbageCommand command;
        private Closer closer;
        private List<String> generatedBasePaths;

        public CreateGarbageCmd(String... args) {
            // append the default mongodb connection string if one was not provided
            if (args[0].startsWith("mongodb://")) {
                this.args = List.copyOf(Arrays.asList(args));
            } else {
                List<String> argsList = new ArrayList<>();
                argsList.add(MongoUtils.URL);
                argsList.addAll(Arrays.asList(args));
                this.args = List.copyOf(argsList);
            }
        }

        @Override
        public void run() {
            try {
                closer = Closer.create();
                command = new CreateGarbageCommand();
                generatedBasePaths = command.execute(closer, true, args.toArray(new String[0]));
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

        public CreateGarbageCommand getCommand() {
            return command;
        }
    }
}
