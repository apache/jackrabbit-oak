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
import org.apache.jackrabbit.oak.spi.query.fulltext.InferenceQuery;
import org.apache.jackrabbit.oak.spi.query.fulltext.InferenceQueryConfig;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStoreService.CACHE_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

/**
 * Configuration class for Inference Model settings.
 * Currently only hybrid search is implemented
 */
public class InferenceModelConfig {
    public static final InferenceModelConfig NOOP = new InferenceModelConfig();
    public static final String MODEL = "model";
    public static final String EMBEDDING_SERVICE_URL = "embeddingServiceUrl";
    public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
    public static final String INFERENCE_PAYLOAD = "inferencePayload";
    // InferenceQueryConfig also uses InferenceModelConfig.TYPE so referencing
    // it from InferenceQueryConfig.
    public static final String TYPE = InferenceQueryConfig.TYPE;
    public static final String MIN_TERMS = "minTerms";
    public static final String IS_DEFAULT = "isDefault";
    public static final String ENABLED = "enabled";
    public static final String HEADER = "header";
    public static final String TIMEOUT = "timeout";
    public static final String PREFIX = "prefix";
    private static final Logger log = LoggerFactory.getLogger(InferenceModelConfig.class);
    private static final String NUM_CANDIDATES = "numCandidates";
    private static final String CACHE_SIZE = "cacheSize";

    private final String model;
    private final String embeddingServiceUrl;
    private final double similarityThreshold;
    private final long minTerms;
    private final boolean isDefault;
    private final boolean enabled;
    private final String type;
    private final InferenceHeaderPayload header;
    private final InferencePayload payload;
    private final String inferenceModelConfigName;


    /**
     * The prefix used for the query. If the input string starts with this prefix, the query will be executed. Default is null (no prefix).
     */
    public String prefix;
    /**
     * The number of candidates to be returned by the query. Default is 100.
     */
    public int numCandidates;
    /**
     * The type of the query. Default is "hybrid". Currently not used
     */
    public String queryType; // this can be hybrid or vector


    /**
     * The timeout for the query in milliseconds. Default is 5000.
     */
    public long timeout;

    public int cacheSize;


    private InferenceModelConfig() {
        this.inferenceModelConfigName = null;
        this.model = null;
        this.embeddingServiceUrl = null;
        this.similarityThreshold = 0.0;
        this.minTerms = 0L;
        this.isDefault = false;
        this.enabled = false;
        this.type = TYPE;
        this.header = null;
        this.payload = null;
    }

    public InferenceModelConfig(String inferenceModelConfigName, NodeState nodeState) {
        this.inferenceModelConfigName = inferenceModelConfigName;
        this.model = nodeState.getProperty(MODEL).getValue(Type.STRING);
        this.embeddingServiceUrl = nodeState.getProperty(EMBEDDING_SERVICE_URL).getValue(Type.STRING);
        this.similarityThreshold = nodeState.getProperty(SIMILARITY_THRESHOLD).getValue(Type.DOUBLE);
        this.minTerms = nodeState.getProperty(MIN_TERMS).getValue(Type.LONG);
        this.isDefault = nodeState.getProperty(IS_DEFAULT).getValue(Type.BOOLEAN);

        this.header = new InferenceHeaderPayload(nodeState.getChildNode(HEADER));
        this.payload = new InferencePayload(inferenceModelConfigName, nodeState.getChildNode(INFERENCE_PAYLOAD));
        this.type = TYPE;
        this.enabled = getOptionalValue(nodeState, ENABLED,false);
        this.timeout = getOptionalValue(nodeState, TIMEOUT, 5000L);
        this.prefix = getOptionalValue(nodeState, PREFIX, "?");
        this.numCandidates = getOptionalValue(nodeState, NUM_CANDIDATES, 100);
        this.cacheSize = getOptionalValue(nodeState, CACHE_SIZE, 100);
    }

    public String getInferenceModelConfigName() {
        return inferenceModelConfigName;
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

    public String getType() {
        return type;
    }

    public InferenceHeaderPayload getHeader() {
        return header;
    }

    public InferencePayload getPayload() {
        return this.payload;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public String getQueryType() {
        return queryType;
    }

    public long getTimeout() {
        return timeout;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public long getTimeoutMillis() {
        return this.timeout;
    }

    @Override
    public String toString() {
        return TYPE + "{" +
                MODEL + "='" + model + '\'' +
                ", " + EMBEDDING_SERVICE_URL + "='" + embeddingServiceUrl + '\'' +
                ", " + SIMILARITY_THRESHOLD + similarityThreshold +
                ", " + MIN_TERMS + "=" + minTerms +
                ", " + IS_DEFAULT + "=" + isDefault +
                ", " + ENABLED + "=" + enabled +
                ", " + HEADER + "=" + header +
                ", " + INFERENCE_PAYLOAD + "=" + payload +
                '}';
    }
}