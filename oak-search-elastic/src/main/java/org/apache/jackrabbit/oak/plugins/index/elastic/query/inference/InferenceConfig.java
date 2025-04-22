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
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

/**
 * Data model class representing the inference configuration stored under /oak:index/:inferenceConfig (default path)
 */
public class InferenceConfig {
    private static final Logger LOG = LoggerFactory.getLogger(InferenceConfig.class.getName());
    private static final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private static final InferenceConfig INSTANCE = new InferenceConfig();
    public static final String TYPE = "inferenceConfig";
    /**
     * Semantic search is enabled if this flag is true
     */
    private boolean enabled;
    /**
     * Map of index names to their respective inference configurations
     */
    private Map<String, InferenceIndexConfig> indexConfigs;
    private NodeStore nodeStore;
    private String inferenceConfigPath;
    private String currentInferenceConfig;
    private static volatile String activeInferenceConfig;

    /**
     * Loads configuration from the given NodeState
     *
     * @return InferenceConfiguration instance
     */

    private InferenceConfig() {
        lock.writeLock().lock();
        try {
            if (INSTANCE == null) {
                LOG.warn("InferenceConfig: NOOP Inference config initialized");
                enabled = false;
                indexConfigs = Map.of();
                activeInferenceConfig = getNewInferenceConfigId();
                currentInferenceConfig = activeInferenceConfig;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static InferenceConfig getInstance(boolean shouldReinitialize) {
        return getInstance(INSTANCE.nodeStore, INSTANCE.inferenceConfigPath, shouldReinitialize);
    }

    public static InferenceConfig getInstance() {
        return getInstance(INSTANCE.nodeStore, INSTANCE.inferenceConfigPath, false);
    }

    /**
     * This method returns the current InferenceConfig instance, we update InferenceConfig if
     * activeInferenceConfig != currentInferenceConfig
     *
     * @param nodeStore
     * @param inferenceConfigPath
     * @return
     */
    public static InferenceConfig getInstance(NodeStore nodeStore, @NotNull String inferenceConfigPath, boolean shouldReinitialize) {

        lock.readLock().lock();
        try {
            if (activeInferenceConfig != null && activeInferenceConfig.equals(INSTANCE.currentInferenceConfig) && !shouldReinitialize) {
                return INSTANCE;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (shouldReinitialize){
                activeInferenceConfig = getNewInferenceConfigId();
            }
            INSTANCE.currentInferenceConfig = activeInferenceConfig;
            INSTANCE.nodeStore = nodeStore;
            INSTANCE.inferenceConfigPath = inferenceConfigPath;
            if (!isValidInferenceConfig(nodeStore, inferenceConfigPath)) {
                INSTANCE.enabled = false;
                INSTANCE.indexConfigs = Map.of();
            } else {
                NodeState nodeState = nodeStore.getRoot();
                for (String elem : PathUtils.elements(inferenceConfigPath)) {
                    nodeState = nodeState.getChildNode(elem);
                }
                // Inference enabled or not.
                INSTANCE.enabled = getOptionalValue(nodeState, InferenceConstants.ENABLED, false);
                Map<String, InferenceIndexConfig> temp_indexConfigs = new HashMap<>();
                // Read index configurations
                for (String indexName : nodeState.getChildNodeNames()) {
                    temp_indexConfigs.put(indexName, new InferenceIndexConfig(indexName, nodeState.getChildNode(indexName)));

                }
                INSTANCE.indexConfigs = Collections.unmodifiableMap(temp_indexConfigs);
                //TODO Check if we we are also logging sensitive info.
                LOG.info("Loaded inference configuration: " + INSTANCE);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return INSTANCE;
    }

    public boolean isEnabled() {
        lock.readLock().lock();
        try {
            return enabled;
        } finally {
            lock.readLock().unlock();
        }
    }

    public @NotNull InferenceIndexConfig getInferenceIndexConfig(String indexName) {
        lock.readLock().lock();
        try {
            getInstance(INSTANCE.nodeStore, INSTANCE.inferenceConfigPath, false);
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
                return inferenceIndexConfig.isEnabled() ? inferenceIndexConfig : InferenceIndexConfig.NOOP;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public @NotNull InferenceModelConfig getInferenceModelConfig(String inferenceIndexName, String inferenceModelConfigName) {
        lock.readLock().lock();
        try {
            getInstance(INSTANCE.nodeStore, INSTANCE.inferenceConfigPath, false);
            InferenceIndexConfig inferenceIndexConfig = getInferenceIndexConfig(inferenceIndexName);
            return inferenceIndexConfig.getInferenceModelConfigs().getOrDefault(inferenceModelConfigName, InferenceModelConfig.NOOP);
        } finally {
            lock.readLock().unlock();
        }

    }

    public @NotNull Map<String, InferenceIndexConfig> getIndexConfigs() {
        lock.readLock().lock();
        try {
            return isEnabled() ?
                    Collections.unmodifiableMap(indexConfigs) : Map.of();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean isValidInferenceConfig(NodeStore nodeStore, String inferenceConfigPath) {

        if (nodeStore == null) {
            LOG.warn("InferenceConfig: NodeStore is null");
            return false;
        }
        NodeState nodeState = nodeStore.getRoot();
        if (inferenceConfigPath == null || inferenceConfigPath.isEmpty()) {
            LOG.warn("InferenceConfig: Inference config path is null or empty");
            return false;
        }

        for (String elem : PathUtils.elements(inferenceConfigPath)) {
            nodeState = nodeState.getChildNode(elem);
            if (!nodeState.exists()) {
                LOG.warn("InferenceConfig: NodeState does not exist for path: " + inferenceConfigPath);
                return false;
            }
        }
        return getOptionalValue(nodeState, InferenceConstants.ENABLED, false);
    }

    private static String getNewInferenceConfigId() {
        return UUID.randomUUID().toString();
    }

} 