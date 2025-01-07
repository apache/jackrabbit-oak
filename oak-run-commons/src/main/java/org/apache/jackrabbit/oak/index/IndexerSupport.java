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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncLaneSwitcher;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class IndexerSupport {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    /**
     * Directory name in output directory under which indexes are
     * stored
     */
    public static final String LOCAL_INDEX_ROOT_DIR = "indexes";
    /**
     * Checkpoint value which indicate that head state needs to be used
     * This would be mostly used for testing purpose
     */
    private static final String HEAD_AS_CHECKPOINT = "head";

    /**
     * Index lane name which is used for indexing
     */
    private static final String REINDEX_LANE = "offline-reindex-async";
    private Map<String, String> checkpointInfo = Collections.emptyMap();
    protected final IndexHelper indexHelper;
    private File localIndexDir;
    private File indexDefinitions;
    private final String checkpoint;
    private File existingDataDumpDir;

    /**
     * The lower bound of the "_modified" property, when using the document node
     * store.
     */
    private long minModified;

    public IndexerSupport(IndexHelper indexHelper, String checkpoint) {
        this.indexHelper = indexHelper;
        this.checkpoint = checkpoint;
    }

    public long getMinModified() {
        return minModified;
    }

    public void setMinModified(long minModified) {
        this.minModified = minModified;
    }

    public IndexerSupport withExistingDataDumpDir(File existingDataDumpDir) {
        this.existingDataDumpDir = existingDataDumpDir;
        return this;
    }

    public File getExistingDataDumpDir() {
        return existingDataDumpDir;
    }

    public File getLocalIndexDir() throws IOException {
        if (localIndexDir == null) {
            localIndexDir = new File(indexHelper.getWorkDir(), LOCAL_INDEX_ROOT_DIR);
            FileUtils.forceMkdir(localIndexDir);
        }
        return localIndexDir;
    }

    public File copyIndexFilesToOutput() throws IOException {
        File destDir = new File(indexHelper.getOutputDir(), getLocalIndexDir().getName());
        FileUtils.moveDirectoryToDirectory(getLocalIndexDir(), indexHelper.getOutputDir(), true);
        return destDir;
    }

    public void writeMetaInfo(String checkpoint) throws IOException {
        new IndexerInfo(getLocalIndexDir(), checkpoint).save();
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public NodeState retrieveNodeStateForCheckpoint() {
        NodeState checkpointedState;
        if (HEAD_AS_CHECKPOINT.equals(checkpoint)) {
            checkpointedState = indexHelper.getNodeStore().getRoot();
            LOG.warn("Using head state for indexing. Such an index cannot be imported back");
        } else {
            checkpointedState = indexHelper.getNodeStore().retrieve(checkpoint);
            requireNonNull(checkpointedState, String.format("Not able to retrieve revision referred via checkpoint [%s]", checkpoint));
            checkpointInfo = indexHelper.getNodeStore().checkpointInfo(checkpoint);
        }
        return checkpointedState;
    }

    public void updateIndexDefinitions(NodeBuilder rootBuilder) throws IOException, CommitFailedException {
        if (indexDefinitions != null) {
            new IndexDefinitionUpdater(indexDefinitions).apply(rootBuilder);
        }
    }

    protected void dumpIndexDefinitions(NodeStore nodeStore) throws IOException {
        IndexDefinitionPrinter printer = new IndexDefinitionPrinter(nodeStore, indexHelper.getIndexPathService());
        printer.setFilter("{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:index-definition\", \"-:data\", \"-:suggest-data\"]}");
        PrinterDumper dumper = new PrinterDumper(getLocalIndexDir(), IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON,
                false, Format.JSON, printer);
        dumper.dump();
    }

    public void switchIndexLanesAndReindexFlag(NodeStore copyOnWriteStore) throws CommitFailedException, IOException {
        NodeState root = copyOnWriteStore.getRoot();
        NodeBuilder builder = root.builder();
        updateIndexDefinitions(builder);

        for (String indexPath : indexHelper.getIndexPaths()) {
            //TODO Do it only for lucene indexes for now
            NodeBuilder idxBuilder = childBuilder(builder, indexPath, false);
            Validate.checkState(idxBuilder.exists(), "No index definition found at path [%s]", indexPath);

            idxBuilder.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
            AsyncLaneSwitcher.switchLane(idxBuilder, REINDEX_LANE);
        }

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        LOG.info("Switched the async lane for indexes at {} to {} and marked them for reindex", indexHelper.getIndexPaths(), REINDEX_LANE);
    }

    public void postIndexWork(NodeStore copyOnWriteStore) throws CommitFailedException, IOException {
        switchIndexLanesBack(copyOnWriteStore);
        dumpIndexDefinitions(copyOnWriteStore);
    }

    protected void switchIndexLanesBack(NodeStore copyOnWriteStore) throws CommitFailedException {
        NodeState root = copyOnWriteStore.getRoot();
        NodeBuilder builder = root.builder();

        for (String indexPath : indexHelper.getIndexPaths()) {
            NodeBuilder idxBuilder = childBuilder(builder, indexPath, false);
            AsyncLaneSwitcher.revertSwitch(idxBuilder, indexPath);
        }

        copyOnWriteStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        LOG.info("Switched the async lane for indexes at {} back to there original lanes", indexHelper.getIndexPaths());
    }

    public Map<String, String> getCheckpointInfo() {
        return checkpointInfo;
    }

    public void setIndexDefinitions(File indexDefinitions) {
        this.indexDefinitions = indexDefinitions;
    }

    public static NodeBuilder childBuilder(NodeBuilder nb, String path, boolean createNew) {
        for (String name : PathUtils.elements(requireNonNull(path))) {
            nb = createNew ? nb.child(name) : nb.getChildNode(name);
        }
        return nb;
    }

    public Set<IndexDefinition> getIndexDefinitions() throws IOException, CommitFailedException {
        NodeState checkpointedState = this.retrieveNodeStateForCheckpoint();
        NodeStore copyOnWriteStore = new MemoryNodeStore(checkpointedState);
        NodeBuilder builder = copyOnWriteStore.getRoot().builder();
        NodeState root = builder.getNodeState();
        this.updateIndexDefinitions(builder);
        IndexDefinition.Builder indexDefBuilder = new IndexDefinition.Builder();

        Set<IndexDefinition> indexDefinitions = new HashSet<>();

        for (String indexPath : indexHelper.getIndexPaths()) {
            NodeBuilder idxBuilder = IndexerSupport.childBuilder(builder, indexPath, false);
            IndexDefinition indexDf = indexDefBuilder.defn(idxBuilder.getNodeState()).indexPath(indexPath).root(root).build();
            indexDefinitions.add(indexDf);
        }
        return indexDefinitions;
    }

    /**
     * @param indexDefinitions
     * @return set of preferred path elements referred from the given set of index definitions.
     */
    public Set<String> getPreferredPathElements(Set<IndexDefinition> indexDefinitions) {
        Set<String> preferredPathElements = new HashSet<>();
        for (IndexDefinition indexDf : indexDefinitions) {
            preferredPathElements.addAll(indexDf.getRelativeNodeNames());
        }
        return preferredPathElements;
    }

    /**
     * @param indexDefinitions     set of IndexDefinition to be used to calculate the Path Predicate
     * @param typeToRepositoryPath Function to convert type <T> to valid repository path of type <String>
     * @param <T>
     * @return filter predicate based on the include/exclude path rules of the given set of index definitions.
     */
    public <T> Predicate<T> getFilterPredicate(Set<IndexDefinition> indexDefinitions, Function<T, String> typeToRepositoryPath) {
        return t -> indexDefinitions.stream().anyMatch(indexDef -> indexDef.getPathFilter().filter(typeToRepositoryPath.apply(t)) != PathFilter.Result.EXCLUDE);
    }

    /**
     * @param pattern              Pattern for a custom excludes regex based on which paths would be filtered out
     * @param typeToRepositoryPath Function to convert type <T> to valid repository path of type <String>
     * @param <T>
     * @return Return a predicate that should test true for all paths that do not match the provided regex pattern.
     */
    public <T> Predicate<T> getFilterPredicateBasedOnCustomRegex(Pattern pattern, Function<T, String> typeToRepositoryPath) {
        return t -> !pattern.matcher(typeToRepositoryPath.apply(t)).find();
    }

    /**
     * Computes the total size of the generated index data. This method is intended to be used when creating Lucene
     * indexes, which are created locally. With Elastic, this will not include the Lucene files since the indexes
     * are updated remotely.
     *
     * @return The total size of the index data generated or -1 if there is some error while computing the size.
     */
    public long computeSizeOfGeneratedIndexData() {
        try {
            File localIndexDir = getLocalIndexDir();
            long totalSize = 0;
            if (localIndexDir == null || !localIndexDir.isDirectory()) {
                LOG.warn("Local index directory is invalid, this should not happen: {}", localIndexDir);
                return -1;
            } else {
                // Each index is stored in a separate directory
                File[] directories = localIndexDir.listFiles(File::isDirectory);
                if (directories == null) {
                    LOG.warn("Error listing sub directories in the local index directory: {}", localIndexDir);
                    return -1;
                }
                // Print the indexes in alphabetic order
                Arrays.sort(directories);
                StringBuilder sb = new StringBuilder();
                for (File indexDir : directories) {
                    long size = FileUtils.sizeOfDirectory(indexDir);
                    totalSize += size;
                    File[] files = indexDir.listFiles(File::isFile);
                    if (files == null) {
                        LOG.warn("Error listing files in directory: {}", indexDir);
                        // continue to the next index
                    } else {
                        long numberOfFiles = files.length;
                        sb.append("\n  - " + indexDir.getName() + ": " + numberOfFiles + " files, " +
                                size + " (" + FileUtils.byteCountToDisplaySize(size) + ")");
                    }
                }
                LOG.info("Total size of index data generated: {} ({}){}",
                        totalSize, FileUtils.byteCountToDisplaySize(totalSize), sb);
                return totalSize;
            }
        } catch (Throwable t) {
            LOG.warn("Error while computing size of generated index data.", t);
            return -1;
        }
    }
}
