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

package org.apache.jackrabbit.oak.spi.query.fulltext;

import org.apache.jackrabbit.oak.json.JsonUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorQuery {
    private static final Logger LOG = LoggerFactory.getLogger(VectorQuery.class);
    private static final String DEFAULT_INFERENCE_QUERY_CONFIG_PREFIX = "?";
    private static final String INFERENCE_QUERY_CONFIG_PREFIX_KEY = "org.apache.jackrabbit.oak.search.inference.query.prefix";
    public static final String INFERENCE_QUERY_CONFIG_PREFIX = System.getProperty(
            INFERENCE_QUERY_CONFIG_PREFIX_KEY, DEFAULT_INFERENCE_QUERY_CONFIG_PREFIX);

    private final String queryInferenceConfig;
    private final String queryText;

    public VectorQuery(@NotNull String text) {
        String[] components = parseText(text);
        this.queryInferenceConfig = components[0];
        this.queryText = components[1];
    }

    private String[] parseText(String inputText) {
        String text = inputText.trim();
        // Remove the first delimiter
        if (text.startsWith(INFERENCE_QUERY_CONFIG_PREFIX) && text.charAt(INFERENCE_QUERY_CONFIG_PREFIX.length()) == '{') {
            text = text.substring(INFERENCE_QUERY_CONFIG_PREFIX.length());

            // Try to find the end of the JSON part by parsing incrementally
            int possibleEndIndex = 0;
            String jsonPart = null;
            String queryTextPart;
            int jsonEndDelimiterIndex = -1;

            while (possibleEndIndex < text.length()) {
                possibleEndIndex = text.indexOf(INFERENCE_QUERY_CONFIG_PREFIX, possibleEndIndex + 1);
                if (possibleEndIndex == -1) {
                    // If we reach here, it means we couldn't find a valid JSON part
                    jsonPart = "";
                    LOG.warn("Query starts with inference prefix {}, but without valid json part," +
                                    " if case this prefix is a valid fulltext query prefix, please update system property {} with different prefix value",
                            INFERENCE_QUERY_CONFIG_PREFIX, INFERENCE_QUERY_CONFIG_PREFIX_KEY);
                    break;
                }
                String candidateJson = text.substring(0, possibleEndIndex);
                // Verify if this is valid JSON using Oak's JsopTokenizer
                if (JsonUtils.isValidJson(candidateJson, false)) {
                    jsonPart = candidateJson;
                    jsonEndDelimiterIndex = possibleEndIndex;
                    break;
                }
            }
            // If we found a valid JSON part, extract it
            if (jsonPart == null) {
                // If we reach here, it means we couldn't find a valid JSON part
                jsonPart = "";
                queryTextPart = text;
                LOG.warn("Query starts with InferenceQueryPrefix: {}, but without valid json part," +
                                " if case this prefix is a valid fulltext query prefix, please update {} with different prefix value",
                        INFERENCE_QUERY_CONFIG_PREFIX, INFERENCE_QUERY_CONFIG_PREFIX_KEY);

            } else {
                // Extract query text part (everything after the JSON part delimiter)
                queryTextPart = text.substring(jsonEndDelimiterIndex + 1).trim();

            }
            return new String[]{jsonPart, queryTextPart};
        } else {
            return new String[]{"", text};
        }
    }

    public String getQueryInferenceConfig() {
        return queryInferenceConfig;
    }

    public String getQueryText() {
        return queryText;
    }
}