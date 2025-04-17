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
import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration for inference payload
 */
public class InferenceHeaderPayload {
    private final Map<String, String> inferenceHeaderPayloadMap;

    public InferenceHeaderPayload(NodeState nodeState) {
        inferenceHeaderPayloadMap = JsonUtils.convertNodeStateToMap(nodeState, 0)
                .entrySet().stream().filter(entry -> entry.getValue() instanceof String)
                .collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), (String) entry.getValue()), HashMap::putAll);
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