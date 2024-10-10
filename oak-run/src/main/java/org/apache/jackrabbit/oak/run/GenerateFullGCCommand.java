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
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.run.Utils.createDocumentMKBuilder;

/**
 * GenerateFullGCCommand generates garbage nodes in the repository in order to allow for testing fullGC functionality.
 */
public class GenerateFullGCCommand implements Command {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateFullGCCommand.class);

    private static final String USAGE = "generateFullGC {<jdbc-uri> | <mongodb-uri>} [options]";

    public final String FULLGC_GEN_BASE_PATH = "/fullGCGenTest/";
    public final String FULLGC_GEN_PARENT_NODE_PREFIX = "fullGCParent_";
    public final String FULLGC_GEN_NODE_PREFIX = "fullGCNode_";

    private int runCount = 0;

    private static class GenerateFullGCOptions extends Utils.NodeStoreOptions {

        final OptionSpec<Integer> createGarbageNodesCount;
        final OptionSpec<Integer> garbageNodesParentCount;
        final OptionSpec<Integer> garbageType;
        final OptionSpec<Long> maxRevisionAgeMillis;
        final OptionSpec<Integer> maxRevisionAgeDelaySeconds;
        final OptionSpec<Integer> numberOfRuns;
        final OptionSpec<Integer> generateIntervalSeconds;

        public GenerateFullGCOptions(String usage) {
            super(usage);
            createGarbageNodesCount = parser
                    .accepts("createGarbageNodesCount", "the total number of garbage nodes to create").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(0);
            garbageNodesParentCount = parser
                    .accepts("garbageNodesParentCount", "total number of parent nodes under which to create garbage nodes").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            garbageType = parser
                    .accepts("garbageType", "garbage type to be generated - must be a value from VersionGarbageCollector.fullGCMode").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(1);
            maxRevisionAgeMillis = parser
                    .accepts("maxRevisionAgeMillis", "the time in the past that the revision must have. This should be identical to the maxRevisionAge " +
                            "used by the fullGC (24h right now) when collecting garbage").withRequiredArg()
                    .ofType(Long.class).defaultsTo(1036800L);
            maxRevisionAgeDelaySeconds = parser
                    .accepts("garbageType", "the time subtracted from  to the timestampAge when generating the timestamps for the garbage. " +
                            "This is in order for the garbage to be collected after a delay after insertion, not right away").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(60);
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

        public int getCreateGarbageNodesCount() {
            return createGarbageNodesCount.value(options);
        }

        public int getGarbageNodesParentCount() {
            return garbageNodesParentCount.value(options);
        }

        public int getGarbageType() {
            return garbageType.value(options);
        }

        public long getMaxRevisionAgeMillis() {
            return maxRevisionAgeMillis.value(options);
        }

        public int getMaxRevisionAgeDelaySeconds() {
            return maxRevisionAgeDelaySeconds.value(options);
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
        runCount = 0;
        try {
            GenerateFullGCOptions options = new GenerateFullGCOptions(USAGE).parse(args);

            if (options.getNumberOfRuns() > 1 && options.getGenerateIntervalSeconds() > 0) {
                generateGarbageContinuously(options, closer);
            } else {
                generateGarbage(options, closer, 0);
            }
        } catch (Throwable e) {
            LOG.error("Command failed", e);
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private void generateGarbageContinuously(GenerateFullGCOptions options, Closer closer) throws IOException, Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        int numberOfRuns = options.getNumberOfRuns();
        int intervalSeconds = options.getGenerateIntervalSeconds();
        Runnable task = () -> {
            if (runCount < numberOfRuns) {
                try {
                    generateGarbage(options, closer, runCount);
                } catch (Exception e) {
                    LOG.error("Error generating garbage in run " + runCount, e);
                }
                LOG.info("Task executed. Count: " + (runCount + 1));
                runCount++;
            } else {
                // Shutdown the executor once the task has run numberOfRuns times
                executor.shutdown();
                LOG.info("Task completed " + numberOfRuns + " times. Stopping execution.");
            }
        };

        // Schedule the task to run every intervalSeconds
        executor.scheduleAtFixedRate(task, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    private void generateGarbage(GenerateFullGCOptions options, Closer closer, int runIndex) throws IOException, Exception {
        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        if (builder == null) {
            System.err.println("generateFullGC mode only available for DocumentNodeStore");
            System.exit(1);
        }

        System.out.println("Generating fullGC on the document: " + FULLGC_GEN_BASE_PATH);
        DocumentNodeStore documentNodeStore = builder.build();

        VersionGarbageCollector.FullGCMode fullGCMode = getFullGCMode(options);
        if (fullGCMode == VersionGarbageCollector.FullGCMode.NONE) {
            LOG.error("Invalid garbageType specified. Must be one of the following: 1 - EMPTYPROPS, 2 - GAP_ORPHANS, 3 - GAP_ORPHANS_EMPTYPROPS");
            System.exit(1);
        }

        //1. Create nodes with properties
        NodeBuilder b1 = documentNodeStore.getRoot().builder();
        String basePath = FULLGC_GEN_BASE_PATH + runIndex;

        b1.child(basePath).setProperty("jcr:primaryType", "nt:file", NAME);

        int nodesCountUnderParent = options.getGarbageNodesParentCount() / options.getGarbageNodesParentCount();
        for(int i = 0; i < options.getGarbageNodesParentCount(); i ++) {
            b1.child(basePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).setProperty("jcr:primaryType", "nt:folder", NAME);

            for(int j = 0; j < nodesCountUnderParent; j ++) {
                b1.child(basePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                        setProperty("jcr:primaryType", "nt:file", NAME);

                if (fullGCMode == VersionGarbageCollector.FullGCMode.EMPTYPROPS || fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS) {
                    b1.child(basePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                            setProperty("prop", "bar", NAME);
                }
            }
        }
        documentNodeStore.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //2. Generate garbage nodes - EMPTY_PROPERTIES
        if (fullGCMode == VersionGarbageCollector.FullGCMode.EMPTYPROPS || fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS) {
            for(int i = 0; i < options.getCreateGarbageNodesCount(); i ++) {
                for(int j = 0; j < nodesCountUnderParent; j ++) {
                    b1.child(basePath).child(FULLGC_GEN_PARENT_NODE_PREFIX + i).child(FULLGC_GEN_NODE_PREFIX + j).
                            setProperty("prop", null, NAME);
                }
            }
        }

        //3.1. Generate garbage nodes - GAP_ORPHANS - remove parent nodes
        StringBuilder sbNodePath = new StringBuilder();
        if (fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS || fullGCMode == VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS) {
            for(int i = 0; i < options.getCreateGarbageNodesCount(); i ++) {
                for(int j = 0; j < nodesCountUnderParent; j ++) {

                    sbNodePath.setLength(0);
                    sbNodePath.append("2:/").append(basePath).append("/").append(FULLGC_GEN_PARENT_NODE_PREFIX).append(i);

                    // Remove parent node
                    documentNodeStore.getDocumentStore().remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES,
                            sbNodePath.toString());
                    documentNodeStore.runBackgroundOperations();
                }
            }
        }
    }

    private VersionGarbageCollector.FullGCMode getFullGCMode(GenerateFullGCOptions options) {
        VersionGarbageCollector.FullGCMode fullGCMode = VersionGarbageCollector.FullGCMode.NONE;
        int garbageType = options.getGarbageType();
        if (garbageType == 1) {
            fullGCMode = VersionGarbageCollector.FullGCMode.EMPTYPROPS;
        } else if (garbageType == 2) {
            fullGCMode = VersionGarbageCollector.FullGCMode.GAP_ORPHANS;
        } else if (garbageType == 3) {
            fullGCMode = VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS;
        }
        return fullGCMode;
    }
}

