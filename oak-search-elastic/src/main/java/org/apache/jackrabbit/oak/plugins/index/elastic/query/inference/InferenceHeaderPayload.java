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

import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.EnvironmentVariableProcessorUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for inference payload
 */
public class InferenceHeaderPayload {
    private static final Logger LOG = LoggerFactory.getLogger(InferenceHeaderPayload.class);

    public static final InferenceHeaderPayload NOOP = new InferenceHeaderPayload();
    private final Map<String, String> inferenceHeaderPayloadMap;

    public InferenceHeaderPayload() {
        this.inferenceHeaderPayloadMap = Map.of();
    }

    public InferenceHeaderPayload(NodeState nodeState) {
        inferenceHeaderPayloadMap = JsonUtils.convertNodeStateToMap(nodeState, 0, false)
            .entrySet().stream().filter(entry -> entry.getValue() instanceof String)
            .filter(entry -> !entry.getKey().equals("jcr:primaryType"))
            .collect(HashMap::new, (map, entry) -> {
                    String value = EnvironmentVariableProcessorUtil.processEnvironmentVariable(
                        InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, (String) entry.getValue(), InferenceConstants.DEFAULT_ENVIRONMENT_VARIABLE_VALUE);
                    map.put(entry.getKey(), value);
                },
                HashMap::putAll);
    }

    /*
     * Get the inference payload as a json string
     *
     * @param text
     * @return
     */
    public Map<String, String> getInferenceHeaderPayload() {
        return inferenceHeaderPayloadMap;
    }

    @Override
    public String toString() {
        return inferenceHeaderPayloadMap.toString();
    }

} 