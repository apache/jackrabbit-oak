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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * EXPERIMENTAL: A service that sends text to an inference service and receives embeddings in return.
 * The embeddings are cached to avoid repeated calls to the inference service.
 */
public class InferenceService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final URI uri;
    private final Cache<String, List<Float>> cache;
    private final HttpClient httpClient;

    public InferenceService(String url, int cacheSize) {
        try {
            this.uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + url, e);
        }
        this.cache = new Cache<>(cacheSize);
        this.httpClient = HttpClient.newHttpClient();
    }

    public List<Float> embeddings(String text, int timeoutMillis) {
        if (cache.containsKey(text)) {
            return cache.get(text);
        }

        try {
            // Create the JSON payload.
            String jsonInputString = "{\"text\":\"" + text + "\"}";

            // Build the HttpRequest.
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(java.time.Duration.ofMillis(timeoutMillis))
                    .header("Content-Type", "application/json; utf-8")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonInputString, StandardCharsets.UTF_8))
                    .build();

            // Send the request and get the response.
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // Parse the response string into a JsonNode.
            JsonNode jsonResponse = MAPPER.readTree(response.body());

            // Extract the 'embedding' property.
            JsonNode embedding = jsonResponse.get("embedding");

            double[] embeddings = MAPPER.treeToValue(embedding, double[].class);

            // Convert the array of doubles to a list of floats.
            List<Float> result = Arrays.stream(embeddings)
                    .mapToObj(d -> ((Double) d).floatValue())
                    .collect(Collectors.toList());

            cache.put(text, result);
            return result;
        } catch (Exception e) {
            throw new InferenceServiceException("Failed to get embeddings", e);
        }
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
