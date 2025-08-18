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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * EXPERIMENTAL: A service that sends text to an inference service and receives embeddings in return.
 * The embeddings are cached to avoid repeated calls to the inference service.
 */
public class InferenceServiceUsingConfig implements InferenceService {

    private static final Logger LOG = LoggerFactory.getLogger(InferenceServiceUsingConfig.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final URI uri;
    private final Cache<String, List<Float>> cache;
    private final HttpClient httpClient;
    private final long timeoutMillis;
    private final InferenceModelConfig inferenceModelConfig;
    private final String[] headersValue;
    private final InferenceServiceMetrics metrics;

    public InferenceServiceUsingConfig(InferenceModelConfig inferenceModelConfig, InferenceServiceMetrics metrics) {
        try {
            this.uri = new URI(inferenceModelConfig.getEmbeddingServiceUrl());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + inferenceModelConfig.getEmbeddingServiceUrl(), e);
        }
        this.cache = new Cache<>(inferenceModelConfig.getCacheSize());
        this.httpClient = HttpClient.newHttpClient();
        this.timeoutMillis = inferenceModelConfig.getTimeoutMillis();
        this.inferenceModelConfig = inferenceModelConfig;
        this.metrics = metrics;
        this.headersValue = inferenceModelConfig.getHeader().getInferenceHeaderPayload()
            .entrySet().stream()
            .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList()).toArray(String[]::new);

    }

    @Override
    public List<Float> embeddings(String text) {
        return embeddings(text, timeoutMillis);
    }

    public List<Float> embeddings(String text, long timeoutMillis) {
        // Track the request
        TimerStats.Context timerContext = metrics.requestStarted();
        long startTime = System.currentTimeMillis();

        if (cache.containsKey(text)) {
            metrics.cacheHit();
            metrics.requestCompleted(System.currentTimeMillis() - startTime, timerContext);
            return cache.get(text);
        }

        metrics.cacheMiss();
        List<Float> result = null;
        try {
            // Create the JSON payload.
            String jsonInputString = inferenceModelConfig.getPayload().getInferencePayload(text);
            List<String> headerValues = inferenceModelConfig.getHeader().getInferenceHeaderPayload()
                .entrySet().stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

            HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(java.time.Duration.ofMillis(timeoutMillis))
                .headers(headersValue)
                .POST(HttpRequest.BodyPublishers.ofString(jsonInputString, StandardCharsets.UTF_8))
                .build();

            // Send the request and get the response.
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                List<Float> embeddingList = new ArrayList<>();
                MAPPER.readTree(response.body()).path("data").get(0).get("embedding").forEach(n -> {
                    embeddingList.add(n.floatValue());
                });
                result = embeddingList;
                cache.put(text, result);
                metrics.requestCompleted(System.currentTimeMillis() - startTime, timerContext);
                return result;
            } else {
                metrics.requestError(System.currentTimeMillis() - startTime, timerContext);
                LOG.error("Failed to get embeddings. Status code: {}, Response: {}", response.statusCode(), response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.requestError(System.currentTimeMillis() - startTime, timerContext);
            throw new InferenceServiceException("Failed to get embeddings", e);
        } catch (IOException e) {
            metrics.requestError(System.currentTimeMillis() - startTime, timerContext);
            throw new InferenceServiceException("Unable to extract embeddings from inference service response", e);
        } finally {
            //TODO evaluate and update how often we want to log these stats.
            // Setting it to log every 10 minutes by default and can be configured using system property.
            metrics.logMetricsSummary(DEFAULT_METRICS_LOGGING_INTERVAL);
        }
        return result;
    }

    private static class Cache<K, V> extends LinkedHashMap<K, V> {
        private final int maxEntries;

        public Cache(int maxEntries) {
            super(maxEntries + 1, 1.0f, true);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxEntries;
        }
    }
}
