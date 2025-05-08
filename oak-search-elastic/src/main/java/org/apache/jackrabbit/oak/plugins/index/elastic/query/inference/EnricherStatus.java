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
import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration for inference payload
 */
public class EnricherStatus {
    private static final Logger LOG = LoggerFactory.getLogger(EnricherStatus.class);

    public static final EnricherStatus NOOP = new EnricherStatus();
    private final Map<String, Object> enricherStatus;

    public EnricherStatus() {
        this.enricherStatus = Map.of();
    }

    public EnricherStatus(NodeStore nodeStore, String inferenceConfigPath) {
        NodeState nodeState = nodeStore.getRoot();
        for (String elem : PathUtils.elements(inferenceConfigPath)) {
            nodeState = nodeState.getChildNode(elem);
            if (!nodeState.exists()) {
                this.enricherStatus = Map.of();
                return;
            }
        }
        for (String node : nodeState.getChildNodeNames()) {
            if (node.equals(InferenceConstants.ENRICH_NODE)) {
                nodeState = nodeState.getChildNode(node);
                enricherStatus = JsonUtils.convertNodeStateToMap(nodeState, 0, false)
                    .entrySet().stream()
                    .filter(entry -> !entry.getKey().equals("jcr:primaryType"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return;
            }
        }
        this.enricherStatus = Map.of();
    }

    /*
     * Get the inference payload as a json string
     *
     * @param text
     * @return
     */
    public Map<String, Object> getEnricherStatus() {
        return enricherStatus;
    }

    @Override
    public String toString() {
        return enricherStatus.toString();
    }

} 