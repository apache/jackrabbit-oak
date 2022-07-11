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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.DEFAULT_NUMBER_OF_SPLIT_STORE_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_USE_LZ4;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_USE_ZIP;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_SPLIT_STORE_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createReader;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

/**
 * This class is being used when {@link FlatFileNodeStoreBuilder.OAK_INDEXER_PARALLEL_INDEX} is set to true.
 * It will split a flat file safely by checking the index definitions. An entry is considered safe to split if only
 * none of the parent directories contains nodes in indexRule and aggregate fields of the provided index definitions.
 */
public class FlatFileSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(FlatFileSplitter.class);

    private static final String SPLIT_DIR_NAME = "split";
    private static final long MINIMUM_SPLIT_THRESHOLD = 10 * FileUtils.ONE_MB;

    private final File workDir;
    private final NodeTypeInfoProvider infoProvider;
    private final File flatFile;
    private final NodeStateEntryReader entryReader;
    private final Compression.Algorithm algorithm;
    private final Set<IndexDefinition> indexDefinitions;
    private Set<String> splitNodeTypeNames;
    private final int splitSize = Integer.getInteger(PROP_SPLIT_STORE_SIZE, DEFAULT_NUMBER_OF_SPLIT_STORE_SIZE);
    private final boolean useCompression = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    private final boolean useLZ4 = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_LZ4, "false"));

    public FlatFileSplitter(File flatFile, File workdir, NodeTypeInfoProvider infoProvider, NodeStateEntryReader entryReader,
            Set<IndexDefinition> indexDefinitions) {
        this.flatFile = flatFile;
        this.indexDefinitions = indexDefinitions;
        this.workDir = new File(workdir, SPLIT_DIR_NAME);

        this.infoProvider = infoProvider;
        this.entryReader = entryReader;

        Compression.Algorithm algorithm = Compression.Algorithm.NONE;
        if (useCompression) {
            algorithm = useLZ4 ?
                    Compression.Algorithm.LZ4 :
                    Compression.Algorithm.GZIP;
        }
        this.algorithm = algorithm;
    }

    private List<File> returnOriginalFlatFile() {
        return Collections.singletonList(flatFile);
    }

    public List<File> split() throws IOException {
        return split(true);
    }

    public List<File> split(boolean deleteOriginal) throws IOException {
        List<File> splitFlatFiles = new ArrayList<>();
        try {
            FileUtils.forceMkdir(workDir);
        } catch (IOException e) {
            LOG.error("failed to create split directory {}", workDir.getAbsolutePath());
            return returnOriginalFlatFile();
        }

        long fileSizeInBytes = flatFile.length();
        long splitThreshold = Math.round((double) (fileSizeInBytes / splitSize));

        // return original if file too small or split size equals 1
        if (splitThreshold < MINIMUM_SPLIT_THRESHOLD || splitSize <= 1) {
            LOG.info("split is not necessary, skip splitting");
            return returnOriginalFlatFile();
        }

        LOG.info("original flat file size: ~{}",
                FileUtils.byteCountToDisplaySize(fileSizeInBytes));
        LOG.info("split threshold is ~{} bytes, estimate split size >={} files",
                FileUtils.byteCountToDisplaySize(splitThreshold), splitSize);

        Set<String>splitNodeTypesName = getSplitNodeTypeNames();
        LOG.info("unsafe split types: {}", splitNodeTypesName);
        if (splitNodeTypesName.contains(NT_BASE)) {
            LOG.info("Skipping split because split node types set contains {}", NT_BASE);
            return returnOriginalFlatFile();
        }

        try (BufferedReader reader = createReader(flatFile, algorithm)) {
            long readPos = 0;
            int outFileIndex = 1;
            File currentFile = new File(workDir, "split-" + outFileIndex + "-" + getSortedStoreFileName(algorithm));
            BufferedWriter writer = createWriter(currentFile, algorithm);
            splitFlatFiles.add(currentFile);

            String line;
            int lineCount = 0;
            Stack<String> nodeTypeNameStack = new Stack<>();
            while ((line = reader.readLine()) != null) {
                updateNodeTypeStack(nodeTypeNameStack, line);
                boolean shouldSplit = (readPos > splitThreshold);
                if (shouldSplit && canSplit(splitNodeTypesName, nodeTypeNameStack)) {
                    writer.close();
                    LOG.info("created split flat file {} with size {}", currentFile.getAbsolutePath(), FileUtils.byteCountToDisplaySize(currentFile.length()));
                    readPos = 0;
                    outFileIndex++;
                    currentFile = new File(workDir, "split-" + outFileIndex + "-" + getSortedStoreFileName(algorithm));
                    writer = createWriter(currentFile, algorithm);
                    splitFlatFiles.add(currentFile);
                    LOG.info("split position found at line {}, creating new split file {}", lineCount, currentFile.getAbsolutePath());
                }
                writer.append(line);
                writer.newLine();
                readPos += line.length() + 1;
                lineCount++;
            }
            writer.close();
            LOG.info("created split flat file {} with size {}", currentFile.getAbsolutePath(), FileUtils.byteCountToDisplaySize(currentFile.length()));

            LOG.info("split total line count: {}", lineCount);
        }

        if (deleteOriginal) {
            LOG.info("removing original flat file {} after splitting into {} files", flatFile.getAbsolutePath(), splitFlatFiles);
            flatFile.delete();
        }

        return splitFlatFiles;
    }

    private void updateNodeTypeStack(Stack<String> parentNodeTypeNames, String line) {
        NodeStateHolder ns = new SimpleNodeStateHolder(line);
        List<String> pathElements = ns.getPathElements();
        int currentLineDepth = pathElements.size();
        int parentTypesDepth = parentNodeTypeNames.size();
        if (currentLineDepth > parentTypesDepth) {
            parentNodeTypeNames.add(getJCRPrimaryType(line));
        } else {
            int popSize = parentTypesDepth - currentLineDepth + 1;
            if (parentTypesDepth > 0) {
                for (int i = 0; i < popSize; i++) {
                    parentNodeTypeNames.pop();
                }
            }
            parentNodeTypeNames.add(getJCRPrimaryType(line));
        }
    }

    private String getJCRPrimaryType(String line) {
        NodeStateEntry nse = entryReader.read(line);
        PropertyState property = nse.getNodeState().getProperty(JCR_PRIMARYTYPE);
        if (property == null) {
            return "";
        }
        Type<?> type = property.getType();
        if (type == Type.NAME) {
            return property.getValue(Type.NAME);
        }
        return "";
    }

    private boolean canSplit(Set<String> nodeTypes, Stack<String> nodeTypeNameStack) {
        if (nodeTypeNameStack.contains("")) {
            return false;
        }
        for (String parentNodeTypeName : nodeTypeNameStack.subList(0, nodeTypeNameStack.size()-1)) {
            if (nodeTypes.contains(parentNodeTypeName)) {
                return false;
            }
        }
        return true;
    }

    private Set<NodeTypeInfo> getSubTypes(String nodeTypeName) {
        Set<NodeTypeInfo> initialSet = new HashSet<>();
        NodeTypeInfo nodeType = infoProvider.getNodeTypeInfo(nodeTypeName);

        Set<String> subTypes = nodeType.getMixinSubTypes();
        subTypes.addAll(nodeType.getPrimarySubTypes());

        for (String subTypeName: subTypes) {
            initialSet.add(infoProvider.getNodeTypeInfo(subTypeName));
            initialSet.addAll(getSubTypes(subTypeName));
        }

        return initialSet;
    }

    public Set<String> getSplitNodeTypeNames() {
        if (splitNodeTypeNames == null) {
            Set<NodeTypeInfo> splitNodeTypes = getSplitNodeType();
            splitNodeTypeNames = splitNodeTypes.stream().map(NodeTypeInfo::getNodeTypeName).collect(Collectors.toSet());
        }
        return splitNodeTypeNames;
    }

    private Set<NodeTypeInfo> getSplitNodeType(){
        HashSet<String> nodeTypeNameSet = new HashSet<>();
        Set<NodeTypeInfo> setOfNodeType = new HashSet<>();

        for (IndexDefinition indexDf : indexDefinitions) {
            Map<String, Aggregate> aggregateMap = indexDf.getAggregates();
            nodeTypeNameSet.addAll(Objects.requireNonNull(aggregateMap).keySet());
            nodeTypeNameSet.addAll(indexDf.getDefinedRules().stream().map(IndexDefinition.IndexingRule::getBaseNodeType).collect(Collectors.toList()));
        }

        for (String nodeTypeName : nodeTypeNameSet) {
            setOfNodeType.add(infoProvider.getNodeTypeInfo(nodeTypeName));
            setOfNodeType.addAll(getSubTypes(nodeTypeName));
        }

        return setOfNodeType;
    }
}
