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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Inference Index settings.
 * Represents the configuration structure for inference-enabled indexes.
 */
public class InferenceIndexConfig {
    private final static Logger LOG = LoggerFactory.getLogger(InferenceIndexConfig.class.getName());
    public static final InferenceIndexConfig NOOP = new InferenceIndexConfig();
    public static final String TYPE = "inferenceIndexConfig";
    public static final String ENRICHER_CONFIG = "enricherConfig";
    public static final String DISABLED_ENRICHER_CONFIG = "";

    /**
     * The enricher configuration as JSON string.
     */
    private final String enricherConfig;
    /**
     * Indicates whether the inference index is enabled or not.
     */
    private volatile boolean isEnabled;
    /**
     * Map of inference model configurations keyed by their names.
     */
    private final Map<String, InferenceModelConfig> inferenceModelConfigs;

    /*

    ES persistence of inference config only happens while creating a new index or reindexing an existing index.
    So enricher config only gets updated on above conditions. Now if disable inferenceIndexConfig the enricher config
    still remains same and enricher will keep on enriching new documents.
    To stop enricher to not enrich new/updated documents one way is to set :enricher status




    Enricher config's value indicates following state:
    {} => Empty enricher config. If we use empty enricher config it means enricher donot need any config to process ES docs.
        In this case we add :enricher.status="PENDING" so enricher can pick these documents for processing.
    "" => if we set enricher config as "", we also add :enricher.status = "COMPLETE"
        :enricher{
            status = "COMPLETE",
            enricherConfig = DISABLED
        }

    Above demarcation in important if we want to disable enriching documents for an index without changing reindexing index.
     */
    private InferenceIndexConfig() {
        this.enricherConfig = DISABLED_ENRICHER_CONFIG;
        this.isEnabled = false;
        this.inferenceModelConfigs = Map.of();
    }

    public InferenceIndexConfig(NodeState nodeState) {
        String tempEnricherConfig;
        boolean tempIsEnabled;
        Map<String, InferenceModelConfig> tempInferenceModelConfigs;
        try {
            tempEnricherConfig = nodeState.hasProperty(InferenceConstants.ENRICHER_CONFIG) ?
                    nodeState.getProperty(InferenceConstants.ENRICHER_CONFIG).getValue(Type.STRING) : "{}";
            tempIsEnabled = nodeState.getProperty(InferenceConstants.ENABLED).getValue(Type.BOOLEAN);
            tempInferenceModelConfigs = new HashMap<>();
            // Iterate through child nodes to find inference model configs
            for (String childName : nodeState.getChildNodeNames()) {
                NodeState childNode = nodeState.getChildNode(childName);
                if (isInferenceModelConfig(childNode)) {
                    tempInferenceModelConfigs.put(childName, new InferenceModelConfig(childName, childNode));
                }
            }
        } catch (Exception e) {
            LOG.error("Error while loading inference index configuration", e);
            tempEnricherConfig = "{}";
            tempIsEnabled = false;
            tempInferenceModelConfigs = Map.of();
        }
        this.enricherConfig = tempEnricherConfig;
        this.inferenceModelConfigs = Collections.unmodifiableMap(tempInferenceModelConfigs);
        this.isEnabled = tempIsEnabled;
    }


    private boolean isInferenceModelConfig(NodeState nodeState) {
        return nodeState.hasProperty(InferenceConstants.TYPE) &&
                nodeState.getProperty(InferenceConstants.TYPE).getValue(Type.STRING).equals(InferenceModelConfig.TYPE);
    }

    /**
     * @return The enricher configuration JSON string
     */
    public String getEnricherConfig() {
        return enricherConfig;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * @return Map of inference model configurations keyed by their names
     */
    public Map<String, InferenceModelConfig> getInferenceModelConfigs() {
        return inferenceModelConfigs;
    }

    /**
     * Gets the enabled default inference model configuration if one exists
     *
     * @return The default InferenceModelConfig.java or null if none is marked as default
     */
    public InferenceModelConfig getDefaultEnabledModel() {
        return inferenceModelConfigs.values().stream()
                .filter(InferenceModelConfig::isDefault)
                .filter(InferenceModelConfig::isEnabled)
                .findFirst()
                .orElse(InferenceModelConfig.NOOP);
    }

    @Override
    public String toString() {
        return TYPE + "{" +
                ENRICHER_CONFIG + "='" + enricherConfig + '\'' +
                ", " + InferenceModelConfig.TYPE + "=" + inferenceModelConfigs +
                '}';
    }

}