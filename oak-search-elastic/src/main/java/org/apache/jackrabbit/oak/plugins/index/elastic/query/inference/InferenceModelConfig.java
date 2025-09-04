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
import org.apache.jackrabbit.oak.plugins.index.elastic.util.EnvironmentVariableProcessorUtil;
import org.apache.jackrabbit.oak.spi.query.fulltext.VectorQueryConfig;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

/**
 * Configuration class for Inference Model settings.
 * Currently only hybrid search is implemented
 */
public class InferenceModelConfig {
    private static final Logger log = LoggerFactory.getLogger(InferenceModelConfig.class);
    private static final String DEFAULT_ENVIRONMENT_VARIABLE_VALUE = "";

    public static final InferenceModelConfig NOOP = new InferenceModelConfig();
    public static final String MODEL = "model";
    public static final String EMBEDDING_SERVICE_URL = "embeddingServiceUrl";
    public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
    public static final String INFERENCE_PAYLOAD = "inferencePayload";
    // InferenceQueryConfig also uses InferenceModelConfig.TYPE so referencing
    // it from InferenceQueryConfig.
    public static final String TYPE = VectorQueryConfig.TYPE;
    public static final String MIN_TERMS = "minTerms";
    public static final String IS_DEFAULT = "isDefault";
    public static final String ENABLED = "enabled";
    public static final String HEADER = "header";
    public static final String TIMEOUT = "timeout";
    public static final String NUM_CANDIDATES = "numCandidates";
    public static final String CACHE_SIZE = "cacheSize";
    private static final double DEFAULT_SIMILARITY_THRESHOLD = 0.8;
    private static final long DEFAULT_MIN_TERMS = 2;
    private static final long DEFAULT_TIMEOUT_MILLIS = 5000L;
    private static final int DEFAULT_NUM_CANDIDATES = 100;
    private static final int DEFAULT_CACHE_SIZE = 100;


    private final String model;
    private final String embeddingServiceUrl;
    private final boolean isDefault;
    private final boolean enabled;
    private final InferenceHeaderPayload header;
    private final InferencePayload payload;
    private final String inferenceModelConfigName;
    private final double similarityThreshold;
    private final long minTerms;
    //The number of candidates to be returned by the query. Default is 100.
    private final int numCandidates;
    //The timeout for the query in milliseconds. Default is 5000.
    private final long timeout;
    private final int cacheSize;


    public InferenceModelConfig() {
        this.isDefault = false;
        this.enabled = false;

        this.model = "";
        this.embeddingServiceUrl = "";
        this.header = InferenceHeaderPayload.NOOP;
        this.payload = InferencePayload.NOOP;
        this.inferenceModelConfigName = "";
        this.similarityThreshold = 0.0;
        this.minTerms = 0L;
        this.numCandidates = 0;
        this.timeout = 0;
        this.cacheSize = DEFAULT_CACHE_SIZE;
    }

    public InferenceModelConfig(String inferenceModelConfigName, NodeState nodeState) {
        this.inferenceModelConfigName = inferenceModelConfigName;
        this.enabled = getOptionalValue(nodeState, InferenceConstants.ENABLED, false);
        boolean isValidType = getOptionalValue(nodeState, InferenceConstants.TYPE, "").equals(InferenceModelConfig.TYPE);
        if (this.enabled && isValidType) {
            this.header = new InferenceHeaderPayload(nodeState.getChildNode(HEADER));
            this.payload = new InferencePayload(inferenceModelConfigName, nodeState.getChildNode(INFERENCE_PAYLOAD));
        } else {
            this.header = InferenceHeaderPayload.NOOP;
            this.payload = InferencePayload.NOOP;
        }
        this.isDefault = getOptionalValue(nodeState, IS_DEFAULT, false);
        this.model = getOptionalValue(nodeState, MODEL, "");
        this.embeddingServiceUrl = EnvironmentVariableProcessorUtil.processEnvironmentVariable(
            InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, getOptionalValue(nodeState, EMBEDDING_SERVICE_URL, ""), DEFAULT_ENVIRONMENT_VARIABLE_VALUE);
        this.similarityThreshold = getOptionalValue(nodeState, SIMILARITY_THRESHOLD, DEFAULT_SIMILARITY_THRESHOLD);
        this.minTerms = getOptionalValue(nodeState, MIN_TERMS, DEFAULT_MIN_TERMS);
        this.timeout = getOptionalValue(nodeState, TIMEOUT, DEFAULT_TIMEOUT_MILLIS);
        this.numCandidates = getOptionalValue(nodeState, NUM_CANDIDATES, DEFAULT_NUM_CANDIDATES);
        this.cacheSize = getOptionalValue(nodeState, CACHE_SIZE, 100);
    }

    @Override
    public String toString() {
        JsopBuilder builder = new JsopBuilder().object().
            key("type").value(TYPE).
            key(MODEL).value(model).
            key(EMBEDDING_SERVICE_URL).value(embeddingServiceUrl).
            key(SIMILARITY_THRESHOLD).encodedValue("" + similarityThreshold).
            key(MIN_TERMS).value(minTerms).
            key(IS_DEFAULT).value(isDefault).
            key(ENABLED).value(enabled).
            key(HEADER).encodedValue(header.toString()).
            key(INFERENCE_PAYLOAD).encodedValue(payload.toString()).
            key(TIMEOUT).value(timeout).
            key(NUM_CANDIDATES).value(numCandidates).
            key(CACHE_SIZE).value(cacheSize);
        builder.endObject();
        return JsopBuilder.prettyPrint(builder.toString());
    }

    public String getInferenceModelConfigName() {
        return inferenceModelConfigName;
    }

    public String getModel() {
        return model;
    }

    public String getEmbeddingServiceUrl() {
        return embeddingServiceUrl;
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

    public InferencePayload getPayload() {
        return payload;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public long getMinTerms() {
        return minTerms;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public long getTimeoutMillis() {
        return timeout;
    }

    public int getCacheSize() {
        return cacheSize;
    }
}