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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

/**
 * Data model class representing the inference configuration stored under /oak:index/:inferenceConfig
 */
public class InferenceConfig {
    Logger LOG = LoggerFactory.getLogger(InferenceConfig.class.getName());

    public static final InferenceConfig NOOP = new InferenceConfig();
    public static final String TYPE = "inferenceConfig";

    private final StampedLock stampedLock = new StampedLock();
    /**
     * Semantic search is enabled if this flag is true
     */
    private volatile boolean enabled;
    /**
     * Map of index names to their respective inference configurations
     */
    private volatile Map<String, InferenceIndexConfig> indexConfigs;
    private NodeStore nodeStore;
    private String inferenceConfigPath;

    /**
     * Loads configuration from the given NodeState
     *
     * @return InferenceConfiguration instance
     */

    private InferenceConfig() {
        LOG.warn("InferenceConfig: NOOP Inference config initialized");
        enabled = false;
        indexConfigs = Map.of();
    }

    /*
     * Constructor to load inference configuration from the given NodeStore and path
     *
     */
    public InferenceConfig(NodeStore nodeStore, String inferenceConfigPath) {
        this.nodeStore = nodeStore;
        this.inferenceConfigPath = inferenceConfigPath;
        if (nodeStore == null) {
            LOG.warn("InferenceConfig: NodeStore is null");
            enabled = false;
            indexConfigs = Collections.emptyMap();
        } else {
            NodeState nodeState = nodeStore.getRoot();
            for (String elem : PathUtils.elements(inferenceConfigPath)) {
                nodeState = nodeState.getChildNode(elem);
                if (!nodeState.exists()) {
                    LOG.warn("InferenceConfig: NodeState does not exist for path: " + inferenceConfigPath);
                    enabled = false;
                    indexConfigs = Collections.emptyMap();
                    return;
                }
            }

            // Inference enabled or not.
            this.enabled = getOptionalValue(nodeState, InferenceConstants.ENABLED,false);
            Map<String, InferenceIndexConfig> temp_indexConfigs = new HashMap<>();

            // Read index configurations
            for (String indexName : nodeState.getChildNodeNames()) {
                if (isValidInferenceIndexConfig(nodeState, indexName)) {
                    temp_indexConfigs.put(indexName, new InferenceIndexConfig(nodeState.getChildNode(indexName)));
                }
            }
            this.indexConfigs = Collections.unmodifiableMap(temp_indexConfigs);
            //TODO Check if we we are also logging sensitive info.
            LOG.info("Loaded inference configuration: " + this.toString());

        }

    }

    private static boolean isValidInferenceIndexConfig(NodeState nodeState, String indexName) {
        return nodeState.getChildNode(indexName).hasProperty(InferenceConstants.TYPE)
                && InferenceIndexConfig.TYPE.equals(nodeState.getChildNode(indexName).getProperty(InferenceConstants.TYPE).getValue(Type.STRING));
    }

    public boolean isEnabled() {
        return enabled;
    }

    public @NotNull InferenceIndexConfig getInferenceIndexConfig(String indexName) {
        if (!isEnabled()) {
            return InferenceIndexConfig.NOOP;
        } else {
            InferenceIndexConfig inferenceIndexConfig;
            IndexName indexNameObject;
            Function<String, InferenceIndexConfig> getInferenceIndexConfig = (iName) ->
                    this.getIndexConfigs().getOrDefault(iName, InferenceIndexConfig.NOOP);
            if (!InferenceIndexConfig.NOOP.equals(inferenceIndexConfig = getInferenceIndexConfig.apply(indexName))) {
                LOG.debug("InferenceIndexConfig for indexName: {} is: {}", indexName, inferenceIndexConfig);
            } else if ((indexNameObject = IndexName.parse(indexName)) != null && indexNameObject.isLegal()
                    && indexNameObject.getBaseName() != null
            ) {
                LOG.debug("InferenceIndexConfig is using baseIndexName {} and is: {}", indexNameObject.getBaseName(), inferenceIndexConfig);
                inferenceIndexConfig = getInferenceIndexConfig.apply(indexNameObject.getBaseName());
            }
            return inferenceIndexConfig;
        }
    }

    public @NotNull InferenceModelConfig getInferenceModelConfig(String inferenceIndexName, String inferenceModelConfigName) {
        InferenceIndexConfig inferenceIndexConfig = getInferenceIndexConfig(inferenceIndexName);
        return inferenceIndexConfig.getInferenceModelConfigs().getOrDefault(inferenceModelConfigName, InferenceModelConfig.NOOP);
    }

    public @NotNull Map<String, InferenceIndexConfig> getIndexConfigs() {
        // Using StampedLock which has better performance for read operations
        long stamp = stampedLock.tryOptimisticRead();

        if (!stampedLock.validate(stamp)) {
            // Fallback to pessimistic read lock if optimistic read fails
            stamp = stampedLock.readLock();
            try {
                return isEnabled() ?
                        Collections.unmodifiableMap(indexConfigs) : Map.of();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        } else {
            // Optimistic read lock succeeded
            return isEnabled() ?
                    Collections.unmodifiableMap(indexConfigs) : Map.of();
        }
    }

    public InferenceConfig refreshConfig() {
        long stamp = stampedLock.writeLock();
        try {
            InferenceConfig refreshedInferenceConfig = new InferenceConfig(this.nodeStore, this.inferenceConfigPath);
            this.enabled = refreshedInferenceConfig.enabled;
            this.indexConfigs = refreshedInferenceConfig.indexConfigs;
            return this;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }
} 