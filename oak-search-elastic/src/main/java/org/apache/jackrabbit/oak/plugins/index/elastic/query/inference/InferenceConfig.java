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
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.HashMap;
import java.util.Map;

/**
 * Data model class representing the inference configuration stored under /oak:index/:inferenceConfig
 */
public class InferenceConfig {
    /**
     * Semantic search is enabled if this flag is true
     */
    private boolean enabled;
    /**
     * Map of index names to their respective inference configurations
     */
    private Map<String, InferenceIndexConfig> indexConfigs;

    /**
     * Loads configuration from the given NodeState
     * @param nodeState NodeState representing :inferenceConfig node
     * @return InferenceConfiguration instance
     */
    public InferenceConfig(NodeState nodeState) {
        
        // Read enabled flag
        PropertyState enabledProp = nodeState.getProperty("enabled");
        this.enabled = enabledProp != null && enabledProp.getValue(Type.BOOLEAN);

        // Read index configurations
        for (String indexName : nodeState.getChildNodeNames()) {
            if (nodeState.getChildNode(indexName).hasProperty("type")
                    && ("InferenceIndexConfig").equals(nodeState.getChildNode(indexName).getProperty("InferenceIndexConfig").getValue(Type.STRING))) {
                NodeState indexNode = nodeState.getChildNode(indexName);
                this.indexConfigs.put(indexName, new InferenceIndexConfig(nodeState.getChildNode(indexName)));
            }
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Map<String, InferenceIndexConfig> getIndexConfigs() {
        return indexConfigs;
    }
} 