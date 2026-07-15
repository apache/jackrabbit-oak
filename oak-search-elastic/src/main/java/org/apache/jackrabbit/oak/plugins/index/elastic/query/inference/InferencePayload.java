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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.EnvironmentVariableProcessorUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for inference payload. We support open-ai api standard
 */
public class InferencePayload {
    public static final InferencePayload NOOP = new InferencePayload();
    private static final Logger LOG = LoggerFactory.getLogger(InferencePayload.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> inferencePayloadMap;
    private final String inputTextKey = "input";

    public InferencePayload() {
        inferencePayloadMap = Map.of();
    }

    public InferencePayload(String inferenceModelName, NodeState nodeState) {
        inferencePayloadMap = JsonUtils.convertNodeStateToMap(nodeState, 0, false);
        inferencePayloadMap.remove("jcr:primaryType");
        Map<String, String> swappedEnvVarsMap = inferencePayloadMap.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof String)
            .collect(HashMap::new, (map, entry) -> {
                    String value = EnvironmentVariableProcessorUtil.processEnvironmentVariable(
                        InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, (String) entry.getValue(), InferenceConstants.DEFAULT_ENVIRONMENT_VARIABLE_VALUE);
                    map.put(entry.getKey(), value);
                },
                HashMap::putAll);
        //replace current keys with swapped
        inferencePayloadMap.putAll(swappedEnvVarsMap);
    }

    /*
     * Get the inference payload as a json string
     *
     * @param text
     * @return
     */
    public String getInferencePayload(String text) {

        // This creates a shallow copy - only the map structure is cloned but values are still references
        Map<String, Object> inferencePayloadMapCopy = new HashMap<>(inferencePayloadMap);
        inferencePayloadMapCopy.put(inputTextKey, List.of(text));
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(inferencePayloadMapCopy);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        JsopBuilder builder = new JsopBuilder().object();
        for (Map.Entry<String, Object> entry : inferencePayloadMap.entrySet()) {
            builder.key(entry.getKey());
            if (entry.getValue() instanceof String) {
                builder.value((String) entry.getValue());
            } else {
                try {
                    builder.encodedValue(objectMapper.writeValueAsString(entry.getValue()));
                } catch (JsonProcessingException e) {
                    LOG.warn("Failed to serialize value for key {}: {}", entry.getKey(), e.getMessage());
                    builder.value(entry.getValue().toString());
                }
            }
        }
        builder.endObject();
        return JsopBuilder.prettyPrint(builder.toString());
    }
} 