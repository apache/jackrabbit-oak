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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
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

    private final URL url;
    private final Cache<String, List<Float>> cache;

    public InferenceService(String url, int cacheSize) {
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid URL: " + url, e);
        }
        this.cache = new Cache<>(cacheSize);
    }

    public List<Float> embeddings(String text) {
        if (cache.containsKey(text)) {
            return cache.get(text);
        }

        HttpURLConnection connection;
        try {
            // Create a connection.
            connection = (HttpURLConnection) url.openConnection();

            // Set the request type to POST.
            connection.setRequestMethod("POST");

            // Set the content type to application/json.
            connection.setRequestProperty("Content-Type", "application/json; utf-8");

            // Enable input and output streams.
            connection.setDoOutput(true);

            // Create the JSON payload.
            String jsonInputString = "{\"text\":\"" + text + "\"}";

            // Write the JSON payload to the output stream of the connection.
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Get the response from the connection.
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }

                // Parse the response string into a JsonNode.
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonResponse = mapper.readTree(response.toString());

                // Extract the 'embedding' property.
                JsonNode embedding = jsonResponse.get("embedding");

                double[] embeddings = mapper.treeToValue(embedding, double[].class);

                // Convert the array of doubles to a list of floats.
                List<Float> result = Arrays.stream(embeddings)
                        .mapToObj(d -> ((Double) d).floatValue())
                        .collect(Collectors.toList());

                cache.put(text, result);
                return result;
            }
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
