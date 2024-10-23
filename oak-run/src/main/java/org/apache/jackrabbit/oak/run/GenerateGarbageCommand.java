/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.run;

import joptsimple.OptionSpec;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.GenerateGarbageHelper;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.run.Utils.createDocumentMKBuilder;

/**
 * GenerateFullGCCommand generates garbage nodes in the repository in order to allow for testing fullGC functionality.
 */
public class GenerateGarbageCommand implements Command, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateGarbageCommand.class);

    private final ScheduledExecutorService continuousRunExecutor = Executors.newScheduledThreadPool(1);

    private static final String USAGE = "createGarbage {<jdbc-uri> | <mongodb-uri>} [options]";

    /**
     * FullGC garbage should be generated under tmp node.
     */
    public static String GARBAGE_GEN_ROOT_PATH_BASE = "tmp";

    /**
     * The root node name for fullGC garbage generation, one level under tmp.
     */
    public static String GARBAGE_GEN_ROOT_NODE_NAME = "fullGC_GarbageRoot";

    /**
     * Root node for fullGC garbage generation.
     * Necessary in order to allow cleanup of all generated garbage nodes by simply removing the root node.
     */
    public static String GARBAGE_GEN_ROOT_PATH = GARBAGE_GEN_ROOT_PATH_BASE + "/" + GARBAGE_GEN_ROOT_NODE_NAME;

    /**
     * Base path for fullGC garbage generation. The timestamp of the run will be appended to this path,
     * which is necessary in order for each garbage generation run to be unique and not overwrite previous ones.
     * If continuous generation is enabled, the index of the run will also be appended to this path.
     */
    public static String GEN_BASE_PATH = "GenTest_";

    /**
     * Prefix for parent nodes under which garbage nodes will be created.
     * The index of the parent node will be appended to this prefix.
     */
    public static String GEN_PARENT_NODE_PREFIX = "Parent_";
    public static String GEN_NODE_PREFIX = "Node_";
    public static String GEN_NODE_LEVEL_PREFIX = "_Level_";

    public static String EMPTY_PROPERTY_NAME = "prop";

    /**
     * The maximum depth in the tree at which to generate gap orphans garbage nodes.
     * Used for validation of the orphansDepth / orphansLevelGap options.
     */
    private final int GAP_ORPHANS_MAX_DEPTH = 15;

    private int continuousRunIndex = 0;

    private DocumentNodeStore documentNodeStore;

    public DocumentNodeStore getDocumentNodeStore() {
        return documentNodeStore;
    }

    private static class GenerateFullGCOptions extends Utils.NodeStoreOptions {

        /**
         * Sub-command for generating garbage.
         * This is the default sub-command to run if none is specified.
         */
        static final String CMD_GENERATE = "generate";

        /**
         * Sub-command for cleaning up all generated garbage.
         * Using this will remove the root node FULLGC_GEN_ROOT_PATH and all of its children (recursively).
         */
        static final String CMD_CLEAN = "clean";

        final OptionSpec<Integer> createGarbageNodesCount;
        final OptionSpec<Integer> garbageNodesParentCount;
        final OptionSpec<Integer> garbageType;
        final OptionSpec<Integer> orphansDepth;
        final OptionSpec<Integer> orphansLevelGap;
        final OptionSpec<Integer> numberOfRuns;
        final OptionSpec<Integer> generateIntervalSeconds;

        public GenerateFullGCOptions(String usage) {
            super(usage);
            createGarbageNodesCount = parser
                    .accepts("garbageNodesCount", "the total number of garbage nodes to create").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(0);
            garbageNodesParentCount = parser
                    .accepts("garbageNodesParentCount", "total number of parent nodes under which to create garbage nodes").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            garbageType = parser
                    .accepts("garbageType", "garbage type to be generated - must be a value from VersionGarbageCollector.fullGCMode").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            orphansDepth = parser
                    .accepts("orphansDepth", "the depth in the tree at which to create garbage nodes. Only applies to ORPHANS fullGC modes").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            orphansLevelGap = parser
                    .accepts("orphansLevelGap", "the gap in the tree between the first leaf gap orphan garbage node and its' parent. Only applies to ORPHANS fullGC modes").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(0);
            numberOfRuns = parser
                    .accepts("numberOfRuns", "the number of garbage generation runs to do. Only applies if greater than 1, " +
                            "otherwise a single run will be done.").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            generateIntervalSeconds = parser
                    .accepts("generateIntervalSeconds", "the interval at which to generate a complete garbage count from createGarbageNotesCount. " +
                            "Applies only if numberOfRuns is greater than 1.").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(60);
        }

        public GenerateFullGCOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        String getSubCmd() {
            List<String> args = getOtherArgs();
            if (args.size() > 0) {
                return args.get(0);
            }
            return CMD_GENERATE;
        }

        public int getCreateGarbageNodesCount() {
            return createGarbageNodesCount.value(options);
        }

        public int getGarbageNodesParentCount() {
            return garbageNodesParentCount.value(options);
        }

        public int getGarbageType() {
            return garbageType.value(options);
        }

        public int getOrphansDepth() { return orphansDepth.value(options);}

        public int getOrphansLevelGap() { return orphansLevelGap.value(options); }

        public int getNumberOfRuns() {
            return numberOfRuns.value(options);
        }

        public int getGenerateIntervalSeconds() {
            return generateIntervalSeconds.value(options);
        }
    }

    public void execute(String... args) throws Exception {

        try (Closer closer = Closer.create()) {
            // Register the ExecutorService with Closer
            closer.register(() -> {
                // Properly shutdown the executor service
                continuousRunExecutor.shutdown();
                try {
                    if (!continuousRunExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        continuousRunExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            execute(closer, args);
        } catch (Throwable e) {
            LOG.error("Command failed", e);
            throw e;
        }
    }

    /**
     * Method with passed closer is necessary in order to allow for unit tests to check the output of the command.
     * It is the responsibility of the caller to close the closer.
     *
     * Returns the list of generated garbage base paths (under the garbage root node).
     * @param closer
     * @param args
     * @throws Exception
     */
    public List<String> execute(Closer closer, String... args) throws Exception {
        continuousRunIndex = 0;

        List<String> generateBasePaths = new ArrayList<>();

        GenerateFullGCOptions options = new GenerateFullGCOptions(USAGE).parse(args);
        String subCmd = options.getSubCmd();

        if (GenerateFullGCOptions.CMD_GENERATE.equals(subCmd)) {

            if (options.getCreateGarbageNodesCount() <= 0) {
                LOG.error("Invalid garbageNodesCount specified: " + options.getCreateGarbageNodesCount() + " in: " + getClass().getName());
                System.exit(1);
            }
            if (options.getGarbageNodesParentCount() <= 0) {
                LOG.error("Invalid garbageNodesParentCount specified: " + options.getGarbageNodesParentCount() + " in: " + getClass().getName());
                System.exit(1);
            }

            if (options.getNumberOfRuns() > 1 && options.getGenerateIntervalSeconds() > 0) {
                generateBasePaths.addAll(generateGarbageContinuously(options, closer));
            } else {
                generateBasePaths.add(generateGarbage(options, closer, 0));
            }
        } else if (GenerateFullGCOptions.CMD_CLEAN.equals(subCmd)) {
            cleanGarbage(options, closer);
        } else {
            System.err.println("Unknown revisions command: " + subCmd);
        }

        return generateBasePaths;
    }

    @Override
    public void close() throws IOException {
        new ExecutorCloser(this.continuousRunExecutor).close();
    }

    private List<String> generateGarbageContinuously(GenerateFullGCOptions options, Closer closer) throws Exception {

        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        long startGenTimestamp = System.currentTimeMillis();

        List<String> generatedGarbageBasePaths = new ArrayList<>();

        int numberOfRuns = options.getNumberOfRuns();
        int intervalSeconds = options.getGenerateIntervalSeconds();
        Runnable task = () -> {
            if (continuousRunIndex < numberOfRuns) {
                try {
                    String genBasePath = generateGarbage(options, continuousRunIndex, builder, startGenTimestamp);
                    generatedGarbageBasePaths.add(genBasePath);
                } catch (Exception e) {
                    LOG.error("Error generating garbage in run " + continuousRunIndex, e);
                }
                LOG.info("Task executed. Count: " + (continuousRunIndex + 1));
                continuousRunIndex++;
            } else {
                // Shutdown the executor once the task has run numberOfRuns times
                continuousRunExecutor.shutdown();
                LOG.info("Task completed " + numberOfRuns + " times. Stopping execution.");
            }
        };

        // Schedule the task to run every intervalSeconds
        continuousRunExecutor.scheduleAtFixedRate(task, 0, intervalSeconds, TimeUnit.SECONDS);

        return generatedGarbageBasePaths;
    }

    /**
     * Generate garbage nodes in the repository in order to allow for testing fullGC functionality.
     *
     * Returns the path of the generated FULLGC_GEN_BASE_PATH node (under the root).
     * @param options
     * @param closer
     * @param runIndex
     * @return
     * @throws IOException
     * @throws Exception
     */
    private String generateGarbage(GenerateFullGCOptions options, Closer closer, int runIndex) throws IOException, Exception {

        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        long generationTimestamp = System.currentTimeMillis();

        return generateGarbage(options, runIndex, builder, generationTimestamp);
    }

    private String generateGarbage(GenerateFullGCOptions options, int runIndex,
                                   DocumentNodeStoreBuilder<?> builder, long timestamp) throws Exception {

        if (builder == null) {
            System.err.println("generateFullGC mode only available for DocumentNodeStore");
            System.exit(1);
        }

        if (GenerateGarbageHelper.isInvalidGarbageGenerationMode(options.getGarbageType())) {
            LOG.error("Invalid fullGCMode specified: " + options.getGarbageType() + " in: " + getClass().getName());
            System.exit(1);
        }

        String generationBasePath = GEN_BASE_PATH + timestamp + "_" + runIndex;
        documentNodeStore = builder.build();

        NodeBuilder rootNode = documentNodeStore.getRoot().builder();

        if (GenerateGarbageHelper.isEmptyProps(options.getGarbageType())) {
            generateGarbageEmptyProps(rootNode, options, generationBasePath);
        }
        else if (GenerateGarbageHelper.isGapOrphans(options.getGarbageType())) {
            generateGarbageGapOrphans(rootNode, options, generationBasePath);
        }

        return generationBasePath;
    }
    private void generateGarbageGapOrphans(NodeBuilder rootNode, GenerateFullGCOptions options, String generationBasePath)
            throws CommitFailedException {

        // validate gap orphans depth and level gap
        if (options.getOrphansDepth() < 0 || options.getOrphansDepth() > GAP_ORPHANS_MAX_DEPTH) {
            LOG.error("Invalid orphansDepth specified: " + options.getOrphansDepth() + " in: " + getClass().getName()
                    + ". Must be an integer >= 0 and <= " + GAP_ORPHANS_MAX_DEPTH);
            System.exit(1);
        }
        if (options.getOrphansLevelGap() < 0 || options.getOrphansLevelGap() > options.getOrphansDepth()) {
            LOG.error("Invalid orphansLevelGap specified: " + options.getOrphansLevelGap() + " in: " + getClass().getName()
                    + ". Must be a positive integer <= orphansDepth");
            System.exit(1);
        }

        System.out.println("Generating fullGC gap orphans on the document: " + generationBasePath);

        NodeBuilder garbageRootNode = rootNode.child(GARBAGE_GEN_ROOT_PATH_BASE).child(GARBAGE_GEN_ROOT_NODE_NAME);
        garbageRootNode.child(generationBasePath).setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, NAME);

        int nodesCountUnderParent = options.getCreateGarbageNodesCount() / options.getGarbageNodesParentCount();
        for (int i = 0; i < options.getGarbageNodesParentCount(); i++) {
            // create parent node
            garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + i).setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:folder", NAME);

            // create child nodes under parent, according to gap orphans depth
            for(int j = 0; j < nodesCountUnderParent; j ++) {
                getGapOrphanLeafGarbageNode(garbageRootNode, generationBasePath, options.getOrphansDepth(), i, j).
                        setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, NAME);
            }
        }
        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();

        // Generate garbage nodes - GAP_ORPHANS - remove parent nodes
        StringBuilder sbNodePath = new StringBuilder();
        List<String> deleteNodePaths = new ArrayList<>();
        for (int i = 0; i < options.getGarbageNodesParentCount(); i++) {

            // append the parent to the paths to delete
            sbNodePath.setLength(0);
            String path = getIdFromPath(
                    sbNodePath.append("/").append(GARBAGE_GEN_ROOT_PATH).append("/").append(generationBasePath).append("/")
                            .append(GEN_PARENT_NODE_PREFIX).append(i).toString());
            deleteNodePaths.add(path);

            // append all the gap orphans nodes between the parent and the level gap to the paths to delete
            for(int j = 0; j < nodesCountUnderParent; j ++) {
                deleteNodePaths.addAll(generateGapOrphanNodePaths(generationBasePath, i, j, options.getOrphansDepth(), options.getOrphansLevelGap(), false));
            }
        }
        // Remove all parent nodes
        documentNodeStore.getDocumentStore().remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, deleteNodePaths);
        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();
    }

    private NodeBuilder getGapOrphanLeafGarbageNode(NodeBuilder garbageRootNode, String generationBasePath, int depth, int parentIndex, int nodeIndex) {
        NodeBuilder garbageNode = garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + parentIndex);
        for (int i = 1; i < depth; i++) {
            garbageNode = garbageNode.child(GEN_NODE_PREFIX + nodeIndex + GEN_NODE_LEVEL_PREFIX + i);
        }
        // the leaf garbage node does not have level in its name
        garbageNode = garbageNode.child(GEN_NODE_PREFIX + nodeIndex);
        return garbageNode;
    }

    public static List<String> generateGapOrphanNodePaths(String generationBasePath, int parentIndex, int nodeIndex, int gapDepth) {
        return generateGapOrphanNodePaths(generationBasePath, parentIndex, nodeIndex, gapDepth, 0, false);
    }

    /**
     * Gets the paths of the nodes between the parent node and the level gap OR between the level gap and the leaf
     * gap orphan node.
     *
     * Example 1: gapDepth = 4, gapLevelGap = 2, fromLevelGapToLeaf = false will return:
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2
     * and will NOT return the rest of the nodes:
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2/Node_0_Level_3
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2/Node_0_Level_3/Node_0 (the leaf node)
     *
     * Example 2: gapDepth = 5, gapLevelGap = 2, fromLevelGapToLeaf = true will return:
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2/Node_0_Level_3
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2/Node_0_Level_3/Node_0_Level_4
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2/Node_0_Level_3/Node_0_Level_4/Node_0 (the leaf node)
     * and will NOT return the nodes from the parent to the gap level:
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1
     * - /tmp/fullGC_GarbageRoot/GenTest_[timestamp]_0/Parent_0/Node_0_Level_1/Node_0_Level_2
     *
     * @param generationBasePath
     * @param parentIndex
     * @param nodeIndex
     * @param gapDepth - the depth in the tree until which to create gap orphan garbage nodes.
     * @param gapLevelGap - the gap in the tree between the first gap orphan garbage node and its parent.
     * @param fromLevelGapToLeaf - if true, the paths will be generated from the level gap to the leaf node, otherwise the paths will be generated from the parent node to the level gap.
     * @return
     */
    public static List<String> generateGapOrphanNodePaths(String generationBasePath, int parentIndex, int nodeIndex, int gapDepth, int gapLevelGap, boolean fromLevelGapToLeaf) {
        List<String> gapOrphanNodePaths = new ArrayList<>();

        StringBuilder sbNodePath = new StringBuilder();
        sbNodePath.setLength(0);

        // start path from the parent node
        sbNodePath.append("/").append(GARBAGE_GEN_ROOT_PATH).append("/").append(generationBasePath).append("/")
                .append(GEN_PARENT_NODE_PREFIX).append(parentIndex);

        for(int i = 1; i <= gapDepth; i ++) {
            sbNodePath.append("/").append(GEN_NODE_PREFIX).append(nodeIndex);
            // the leaf node does NOT have level in its name
            if (i < gapDepth) {
                sbNodePath.append(GEN_NODE_LEVEL_PREFIX).append(i);
            }

            // only append to list if the criteria for returning the node path is met
            if ((!fromLevelGapToLeaf && i < gapLevelGap) || (fromLevelGapToLeaf && i >= gapLevelGap)) {
                gapOrphanNodePaths.add(getIdFromPath(sbNodePath.toString()));
            }
        }

        return gapOrphanNodePaths;
    }

    private void generateGarbageEmptyProps(NodeBuilder rootNode, GenerateFullGCOptions options, String generationBasePath) throws CommitFailedException {

        System.out.println("Generating fullGC empty props on the document: " + generationBasePath);

        NodeBuilder garbageRootNode = rootNode.child(GARBAGE_GEN_ROOT_PATH_BASE).child(GARBAGE_GEN_ROOT_NODE_NAME);
        garbageRootNode.child(generationBasePath).setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, NAME);

        int nodesCountUnderParent = options.getCreateGarbageNodesCount() / options.getGarbageNodesParentCount();
        for(int i = 0; i < options.getGarbageNodesParentCount(); i ++) {
            garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + i).setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:folder", NAME);

            for(int j = 0; j < nodesCountUnderParent; j ++) {
                garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + i).child(GEN_NODE_PREFIX + j).
                        setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, NAME);

                if (GenerateGarbageHelper.isEmptyProps(options.getGarbageType())) {
                    garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + i).child(GEN_NODE_PREFIX + j).
                            setProperty(EMPTY_PROPERTY_NAME, "bar", STRING);
                }
            }
        }
        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();


        //2. Generate garbage nodes - EMPTY_PROPERTIES
        for (int i = 0; i < options.getGarbageNodesParentCount(); i++) {
            for (int j = 0; j < nodesCountUnderParent; j++) {
                garbageRootNode.child(generationBasePath).child(GEN_PARENT_NODE_PREFIX + i).child(GEN_NODE_PREFIX + j).
                        removeProperty(EMPTY_PROPERTY_NAME);
            }
        }

        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();
    }

    /**
     * Cleans up all generated garbage by removing the node FULLGC_GEN_ROOT_PATH and all of
     * its children (recursively)
     * @param options
     * @param closer
     * @throws IOException
     * @throws Exception
     */
    private void cleanGarbage(GenerateFullGCOptions options, Closer closer) throws IOException, Exception {

        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);

        if (builder == null) {
            System.err.println("generateFullGC mode only available for DocumentNodeStore");
            System.exit(1);
        }

        System.out.println("Cleaning up all generated garbage:");
        documentNodeStore = builder.build();

        NodeBuilder rootBuilder = documentNodeStore.getRoot().builder();

        NodeBuilder generatedGarbageRootBuilder = rootBuilder.child(GARBAGE_GEN_ROOT_PATH_BASE).child(GARBAGE_GEN_ROOT_NODE_NAME);

        String garbageRootNodePath = "2:/"+ GARBAGE_GEN_ROOT_PATH;
        List<String> childNodePaths = new ArrayList<>();
        childNodePaths.add(garbageRootNodePath);

        // get all paths of the tree nodes under the garbage root node
        getTreeNodePaths(generatedGarbageRootBuilder, garbageRootNodePath, childNodePaths, 2);

        documentNodeStore.getDocumentStore().remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, childNodePaths);
        documentNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();
    }

    /**
     * Recursively get all paths of the tree nodes under the given root node.
     * @param rootNode
     * @param basePath
     * @param treeNodePaths
     * @param nodeLevel
     */
    private void getTreeNodePaths(NodeBuilder rootNode, String basePath,
                                  List<String> treeNodePaths, int nodeLevel) {
        String childBasePath = basePath.replaceFirst(nodeLevel + ":/", (nodeLevel + 1) + ":/");
        for (String childNodeName : rootNode.getChildNodeNames()) {
            String childPath = childBasePath + "/" + childNodeName;
            treeNodePaths.add(childPath);
            getTreeNodePaths(rootNode.child(childNodeName), childPath, treeNodePaths, nodeLevel + 1);
        }
    }
}

