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
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexerSupport {
    private final Logger log = LoggerFactory.getLogger(getClass());
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
    private Map<String, String> checkpointInfo = Collections.emptyMap();
    private final IndexHelper indexHelper;
    private File localIndexDir;
    private File indexDefinitions;
    private String checkpoint;

    public IndexerSupport(IndexHelper indexHelper, String checkpoint) {
        this.indexHelper = indexHelper;
        this.checkpoint = checkpoint;
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

    public NodeState retrieveNodeStateForCheckpoint() {
        NodeState checkpointedState;
        if (HEAD_AS_CHECKPOINT.equals(checkpoint)) {
            checkpointedState = indexHelper.getNodeStore().getRoot();
            log.warn("Using head state for indexing. Such an index cannot be imported back");
        } else {
            checkpointedState = indexHelper.getNodeStore().retrieve(checkpoint);
            checkNotNull(checkpointedState, "Not able to retrieve revision referred via checkpoint [%s]", checkpoint);
            checkpointInfo = indexHelper.getNodeStore().checkpointInfo(checkpoint);
        }
        return checkpointedState;
    }

    public void updateIndexDefinitions(NodeState root, NodeBuilder builder) throws IOException, CommitFailedException {
        if (indexDefinitions != null) {
            new IndexDefinitionUpdater(indexDefinitions).apply(root, builder);
        }
    }

    public void dumpIndexDefinitions(NodeStore nodeStore) throws IOException, CommitFailedException {
        IndexDefinitionPrinter printer = new IndexDefinitionPrinter(nodeStore, indexHelper.getIndexPathService());
        printer.setFilter("{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:index-definition\"]}");
        PrinterDumper dumper = new PrinterDumper(getLocalIndexDir(), IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON,
                false, Format.JSON, printer);
        dumper.dump();
    }

    public Map<String, String> getCheckpointInfo() {
        return checkpointInfo;
    }

    public void setIndexDefinitions(File indexDefinitions) {
        this.indexDefinitions = indexDefinitions;
    }
}
