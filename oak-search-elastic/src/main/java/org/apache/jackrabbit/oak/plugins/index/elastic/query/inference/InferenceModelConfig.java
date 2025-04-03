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

/**
 * Configuration class for Inference Model settings.
 */
public class InferenceModelConfig {
    private final String model;
    private final String embeddingServiceUrl;
    private final double similarityThreshold;
    private final long minTerms;
    private final boolean isDefault;
    private final boolean enabled;
    private final String type;
    private final InferenceHeaderPayload header;
    private final InferencePayload payload;

    public InferenceModelConfig(NodeState nodeState) {
        this.model = nodeState.getProperty("model").getValue(Type.STRING);
        this.embeddingServiceUrl = nodeState.getProperty("embeddingServiceUrl").getValue(Type.STRING);
        this.similarityThreshold = nodeState.getProperty("similarityThreshold").getValue(Type.DOUBLE);
        this.minTerms = nodeState.getProperty("minTerms").getValue(Type.LONG);
        this.isDefault = nodeState.getProperty("isDefault").getValue(Type.BOOLEAN);
        this.enabled = nodeState.getProperty("enabled").getValue(Type.BOOLEAN);
        this.header = new InferenceHeaderPayload(nodeState.getChildNode("header"));
        this.payload = new InferencePayload(nodeState.getChildNode("inferencepayload"));
        this.type = "InferenceModelConfig";
    }

    // Getters
    public String getModel() {
        return model;
    }

    public String getEmbeddingServiceUrl() {
        return embeddingServiceUrl;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public long getMinTerms() {
        return minTerms;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public InferenceHeaderPayload getHeader() {
        return header;
    }

    @Override
    public String toString() {
        return "InferenceModelConfig{" +
                "model='" + model + '\'' +
                ", embeddingServiceUrl='" + embeddingServiceUrl + '\'' +
                ", similarityThreshold=" + similarityThreshold +
                ", minTerms=" + minTerms +
                ", isDefault=" + isDefault +
                ", enabled=" + enabled +
                ", header=" + header +
                ", payload=" + payload +
                '}';
    }
}