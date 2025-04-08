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
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
/**
 * Configuration for inference payload
 */
public class InferencePayload {
    public static final String TEXT_KEY = "textKey";
    NodeBuilder inferencePayloadBuilder;
    String textKeyValue;
    public InferencePayload(NodeState nodeState) {
        inferencePayloadBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        copyFirstLevelNodeState(nodeState, inferencePayloadBuilder);
        textKeyValue = inferencePayloadBuilder.getProperty(TEXT_KEY).getValue(Type.STRING);
        inferencePayloadBuilder.setProperty(textKeyValue, "");
        inferencePayloadBuilder.removeProperty(TEXT_KEY);
    }

    private static void copyFirstLevelNodeState(NodeState source, NodeBuilder target) {
        // Copy properties
        for (PropertyState property : source.getProperties()) {
            target.setProperty(property);
        }
    }

    /* 
     * Get the inference payload as a json string
     * 
     * @param text
     * @return
     */
    public String getInferencePayload(String text) {
        inferencePayloadBuilder.setProperty(textKeyValue, text);
        return inferencePayloadBuilder.getNodeState().toString();
    }

} 