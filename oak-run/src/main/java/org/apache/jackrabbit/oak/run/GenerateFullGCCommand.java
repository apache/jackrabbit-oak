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
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.run.Utils.createDocumentMKBuilder;

/**
 * GenerateFullGCCommand generates garbage nodes in the repository in order to allow for testing fullGC functionality.
 */
public class GenerateFullGCCommand implements Command {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateFullGCCommand.class);

    private static final String USAGE = "generateFullGC {<jdbc-uri> | <mongodb-uri>} [options]";

    /**
     * Root node for fullGC garbage generation.
     * Necessary in order to allow cleanup of all generated garbage nodes by simply removing the root node.
     */
    public static String FULLGC_GEN_ROOT_PATH = "fullGCGenRoot";

    /**
     * Base path for fullGC garbage generation. The timestamp of the run will be appended to this path,
     * which is necessary in order for each garbage generation run to be unique and not overwrite previous ones.
     * If continuous generation is enabled, the index of the run will also be appended to this path.
     */
    public static String FULLGC_GEN_BASE_PATH = "fullGCGenTest_";

    /**
     * Prefix for parent nodes under which garbage nodes will be created.
     * The index of the parent node will be appended to this prefix.
     */
    public static String FULLGC_GEN_PARENT_NODE_PREFIX = "fullGCParent_";
    public static String FULLGC_GEN_NODE_PREFIX = "fullGCNode_";

    public static String EMPTY_PROPERTY_NAME = "prop";

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

        public int getNumberOfRuns() {
            return numberOfRuns.value(options);
        }

        public int getGenerateIntervalSeconds() {
            return generateIntervalSeconds.value(options);
        }
    }

    public void execute(String... args) throws Exception {
        Closer closer = Closer.create();
        try {
            execute(closer, args);
        } catch (Throwable e) {
            LOG.error("Command failed", e);
            throw closer.rethrow(e);
        } finally {
            closer.close();
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
            if (options.getNumberOfRuns() > 1 && options.getGenerateIntervalSeconds() > 0) {
                generateBasePaths.addAll(generateGarbageContinuously(options, closer));
            } else {
                generateBasePaths.add(generateGarbage(options, closer, 0));
            }
        } else if (GenerateFullGCOptions.CMD_CLEAN.equals(subCmd)) {
            cleanGarbage(options, closer);
        } else {
            System.err.println("unknown revisions command: " + subCmd);
        }

        return generateBasePaths;
    }

    private List<String> generateGarbageContinuously(GenerateFullGCOptions options, Closer closer) throws IOException, Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        long startGenTimestamp = System.currentTimeMillis();

        List<String> generatedGarbageBasePaths = new ArrayList<>();

        int numberOfRuns = options.getNumberOfRuns();
        int intervalSeconds = options.getGenerateIntervalSeconds();
        Runnable task = () -> {
            if (continuousRunIndex < numberOfRuns) {
                try {
                    String genBasePath = generateGarbage(options, closer, continuousRunIndex, builder, startGenTimestamp);
                    generatedGarbageBasePaths.add(genBasePath);
                } catch (Exception e) {
                    LOG.error("Error generating garbage in run " + continuousRunIndex, e);
                }
                LOG.info("Task executed. Count: " + (continuousRunIndex + 1));
                continuousRunIndex++;
            } else {
                // Shutdown the executor once the task has run numberOfRuns times
                executor.shutdown();
                LOG.info("Task completed " + numberOfRuns + " times. Stopping execution.");
            }
        };

        // Schedule the task to run every intervalSeconds
        executor.scheduleAtFixedRate(task, 0, intervalSeconds, TimeUnit.SECONDS);

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

        return generateGarbage(options, closer, runIndex, builder, generationTimestamp);
    }

    private String generateGarbage(GenerateFullGCOptions options, Closer closer, int runIndex,
                                  DocumentNodeStoreBuilder<?> builder, long timestamp) throws IOException, Exception {

        if (builder == null) {
            System.err.println("generateFullGC mode only available for DocumentNodeStore");
            System.exit(1);
        }

        String generationBasePath = FULLGC_GEN_BASE_PATH + timestamp + "_" + runIndex;
        System.out.println("Generating fullGC on the document: " + generationBasePath);
        documentNodeStore = builder.build();

        VersionGarbageCollector.FullGCMode fullGCMode = getFullGCMode(options);
        if (fullGCMode == VersionGarbageCollector.FullGCMode.NONE) {
            LOG.error("Invalid garbageType specified. Must be one of the following: 1 - EMPTYPROPS, 2 - GAP_ORPHANS, 3 - GAP_ORPHANS_EMPTYPROPS");
            System.exit(1);
        }

        //1. Create nodes with properties
        NodeBuilder rootNode = documentNodeStore.getRoot().builder();
        NodeBuilder garbageRootNode = rootNode.child(FULLGC_GEN_ROOT_PATH);
        garbageRootNode.child(generationBasePath).setProperty("jcr:primaryType", "nt:file", NAME);

        int nodesCountUnderParent = options.getCreateGarbageNodesCount() / options.getGarbageNodesParentCount();
        for(int i = 0; i < options.getGarbageNodesParentCount(); i ++) {
            garbageRootNode.child(generationBasePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).setProperty("jcr:primaryType", "nt:folder", NAME);

            for(int j = 0; j < nodesCountUnderParent; j ++) {
                garbageRootNode.child(generationBasePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                        setProperty("jcr:primaryType", "nt:file", NAME);

                if (fullGCMode == VersionGarbageCollector.FullGCMode.EMPTYPROPS || fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS) {
                    garbageRootNode.child(generationBasePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                            setProperty(EMPTY_PROPERTY_NAME, "bar", STRING);
                }
            }
        }
        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();


        //2. Generate garbage nodes - EMPTY_PROPERTIES
        if (fullGCMode == VersionGarbageCollector.FullGCMode.EMPTYPROPS) {
            for (int i = 0; i < options.getGarbageNodesParentCount(); i++) {
                for (int j = 0; j < nodesCountUnderParent; j++) {
                    garbageRootNode.child(generationBasePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                            removeProperty(EMPTY_PROPERTY_NAME);
                }
            }
        }
        documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        documentNodeStore.runBackgroundOperations();

        //3.1. Generate garbage nodes - GAP_ORPHANS - remove parent nodes
        if (fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS) {
            StringBuilder sbNodePath = new StringBuilder();
            List<String> deleteNodePaths = new ArrayList<>();
            for (int i = 0; i < options.getGarbageNodesParentCount(); i++) {

                sbNodePath.setLength(0);
                sbNodePath.append("3:/").append(FULLGC_GEN_ROOT_PATH).append("/").append(generationBasePath).append("/").
                        append(FULLGC_GEN_PARENT_NODE_PREFIX).append(i);
                deleteNodePaths.add(sbNodePath.toString());
            }
            // Remove all parent nodes
            documentNodeStore.getDocumentStore().remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, deleteNodePaths);
            documentNodeStore.merge(rootNode, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            documentNodeStore.runBackgroundOperations();
        }

        return generationBasePath;
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

        NodeBuilder generatedGarbageRootBuilder = rootBuilder.child(FULLGC_GEN_ROOT_PATH);

        String garbageRootNodePath = "1:/"+FULLGC_GEN_ROOT_PATH;
        List<String> childNodePaths = new ArrayList<>();
        childNodePaths.add(garbageRootNodePath);

        // get all paths of the tree nodes under the garbage root node
        getTreeNodePaths(generatedGarbageRootBuilder, garbageRootNodePath, childNodePaths, 1);

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
    public <T extends Document> T getDocument(Collection<T> collection, String key, int maxCacheAge)
            throws DocumentStoreException {
        return documentNodeStore.getDocumentStore().find(collection, key, maxCacheAge);
    }

    /**
     * Get the fullGC mode based on the garbageType specified in the options.
     * There is no need to support GAP_ORPHANS_EMPTYPROPS as 2 separate runs of the tool can be done
     * for GAP_ORPHANS and EMPTYPROPS in order to generate both kinds of garbage.
     * @param options
     * @return
     */
    private VersionGarbageCollector.FullGCMode getFullGCMode(GenerateFullGCOptions options) {
        VersionGarbageCollector.FullGCMode fullGCMode = VersionGarbageCollector.FullGCMode.NONE;
        int garbageType = options.getGarbageType();

        if (garbageType == 1) {
            fullGCMode = VersionGarbageCollector.FullGCMode.EMPTYPROPS;
        } else if (garbageType == 2) {
            fullGCMode = VersionGarbageCollector.FullGCMode.GAP_ORPHANS;
        }
        return fullGCMode;
    }
}

