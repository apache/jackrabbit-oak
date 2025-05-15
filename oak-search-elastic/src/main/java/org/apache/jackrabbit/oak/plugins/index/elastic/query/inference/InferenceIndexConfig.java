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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

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
    private boolean isEnabled;
    /**
     * Map of inference model configurations keyed by their names.
     */
    private final Map<String, InferenceModelConfig> inferenceModelConfigs;

    private InferenceIndexConfig() {
        this.enricherConfig = DISABLED_ENRICHER_CONFIG;
        this.isEnabled = false;
        this.inferenceModelConfigs = Map.of();
    }

    public InferenceIndexConfig(String indexName, NodeState nodeState) {
        boolean isValidType = getOptionalValue(nodeState, InferenceConstants.TYPE, "").equals(InferenceIndexConfig.TYPE);
        this.isEnabled = getOptionalValue(nodeState, InferenceConstants.ENABLED, false);
        String enricherString = getOptionalValue(nodeState, InferenceConstants.ENRICHER_CONFIG, "");
        boolean isValidEnricherConfig = JsonUtils.isValidJson(enricherString, false);

        if (isValidType && isEnabled && isValidEnricherConfig) {
            this.enricherConfig = enricherString;
            Map<String, InferenceModelConfig> tempInferenceModelConfigs = new HashMap<>();
            // Iterate through child nodes to find inference model configs
            for (String childName : nodeState.getChildNodeNames()) {
                NodeState childNode = nodeState.getChildNode(childName);
                tempInferenceModelConfigs.put(childName, new InferenceModelConfig(childName, childNode));
            }
            inferenceModelConfigs = Collections.unmodifiableMap(tempInferenceModelConfigs);
        } else {
            this.isEnabled = false;
            this.enricherConfig = getOptionalValue(nodeState, InferenceConstants.ENRICHER_CONFIG, DISABLED_ENRICHER_CONFIG);
            inferenceModelConfigs = Map.of();
            LOG.warn("inference index config for indexName: {} is not valid. Node: {}",
                indexName, nodeState);
        }
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
        JsopBuilder builder = new JsopBuilder().object().
            key("type").value(TYPE).
            key(ENRICHER_CONFIG).value(enricherConfig).
            key(InferenceConstants.ENABLED).value(isEnabled).
            key("inferenceModelConfigs").object();

        // Serialize each model config
        for (Map.Entry<String, InferenceModelConfig> e : inferenceModelConfigs.entrySet()) {
            builder.key(e.getKey()).encodedValue(e.getValue().toString());
        }
        builder.endObject().endObject();
        return JsopBuilder.prettyPrint(builder.toString());
    }

}