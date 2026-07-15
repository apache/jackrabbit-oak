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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for inference payload
 */
public class EnricherStatus {
    private static final Logger LOG = LoggerFactory.getLogger(EnricherStatus.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final EnricherStatus NOOP = new EnricherStatus();
    private final Map<String, Object> enricherStatusData;
    private final String enricherStatusJsonMapping;

    public EnricherStatus() {
        this.enricherStatusData = Map.of();
        this.enricherStatusJsonMapping = "{}";
    }

    public EnricherStatus(NodeStore nodeStore, String inferenceConfigPath) {
        String enricherStatusJsonMapping = "{}";
        Map<String, Object> enricherStatusData = Map.of();
        if (nodeStore != null) {
            NodeState nodeState = nodeStore.getRoot();
            for (String elem : PathUtils.elements(inferenceConfigPath)) {
                nodeState = nodeState.getChildNode(elem);
                if (!nodeState.exists()) {
                    this.enricherStatusJsonMapping = "{}";
                    this.enricherStatusData = Map.of();
                    return;
                }
            }
            try {
                for (String node : nodeState.getChildNodeNames()) {
                    if (node.equals(InferenceConstants.ENRICH_NODE)) {
                        NodeState enrichNode = nodeState.getChildNode(node);
                        String enricherStatusJson = enrichNode.getString(InferenceConstants.ENRICHER_STATUS_DATA);
                        enricherStatusData = MAPPER.readValue(enricherStatusJson, new TypeReference<HashMap<String, Object>>() {
                        });
                        enricherStatusJsonMapping = enrichNode.getString(InferenceConstants.ENRICHER_STATUS_MAPPING);
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse enricher status data: {}", e.getMessage());
            }
        }
        this.enricherStatusJsonMapping = enricherStatusJsonMapping;
        this.enricherStatusData = enricherStatusData;
    }

    public Map<String, Object> getEnricherStatus() {
        return enricherStatusData;
    }

    public String getEnricherStatusJsonMapping() {
        return enricherStatusJsonMapping;
    }

    @Override
    public String toString() {
        JsopBuilder builder = new JsopBuilder().object();
        // Add the mapping data
        builder.key(InferenceConstants.ENRICHER_STATUS_MAPPING).value(enricherStatusJsonMapping);

        // Add enricher status data
        builder.key(InferenceConstants.ENRICHER_STATUS_DATA).object();
        for (Map.Entry<String, Object> entry : enricherStatusData.entrySet()) {
            builder.key(entry.getKey());
            if (entry.getValue() instanceof String) {
                builder.value((String) entry.getValue());
            } else {
                try {
                    builder.encodedValue(MAPPER.writeValueAsString(entry.getValue()));
                } catch (JsonProcessingException e) {
                    LOG.warn("Failed to serialize value for key {}: {}", entry.getKey(), e.getMessage());
                    builder.value(entry.getValue().toString());
                }
            }
        }
        builder.endObject().endObject();
        return JsopBuilder.prettyPrint(builder.toString());
    }
} 