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
    public static final String INFERENCE_QUERY_CONFIG_PREFIX_KEY = "org.apache.jackrabbit.oak.search.inference.query.prefix";
    public static final String INFERENCE_QUERY_CONFIG_PREFIX = System.getProperty(
        INFERENCE_QUERY_CONFIG_PREFIX_KEY, DEFAULT_INFERENCE_QUERY_CONFIG_PREFIX);
    public static final String EXPERIMENTAL_COMPATIBILITY_MODE_KEY = "oak.inference.experimental.compatibility";
    private static boolean isCompatibilityModeEnabled = Boolean.getBoolean(EXPERIMENTAL_COMPATIBILITY_MODE_KEY);

    private final String queryInferenceConfig;
    private final String queryText;

    public VectorQuery(@NotNull String text) {
        String[] components = parseText(text);
        this.queryInferenceConfig = components[0];
        this.queryText = components[1];
    }

    private String[] parseText(String inputText) {
        String jsonPart = null;
        String queryTextPart = null;
        String text = inputText.trim();
        if (text.startsWith(INFERENCE_QUERY_CONFIG_PREFIX)) {
            text = text.substring(INFERENCE_QUERY_CONFIG_PREFIX.length());
            if (text.charAt(0) == '{') {
                // Try to find the end of the JSON part by parsing incrementally
                int possibleEndIndex = 0;
                int jsonEndDelimiterIndex = -1;
                while (possibleEndIndex < text.length()) {
                    possibleEndIndex = text.indexOf(INFERENCE_QUERY_CONFIG_PREFIX, possibleEndIndex + INFERENCE_QUERY_CONFIG_PREFIX.length());
                    if (possibleEndIndex == -1) {
                        // If we reach here, it means we couldn't find a valid JSON part
                        jsonPart = "{}";
                        // we should now use text string as queryText
                        jsonEndDelimiterIndex = 0;
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
                text = text.substring(jsonEndDelimiterIndex);
                if (text.startsWith(INFERENCE_QUERY_CONFIG_PREFIX)) {
                    // Remove the second delimiter
                    text = text.substring(INFERENCE_QUERY_CONFIG_PREFIX.length());
                }
                queryTextPart = text;
            } else {
                if (isCompatibilityModeEnabled) {
                    // No JSON part present but starts with prefix
                    // we return "{}" to be compatible with experimental inference queries
                    jsonPart = "{}";
                    queryTextPart = text;
                } else {
                    jsonPart = "";
                    queryTextPart = inputText;
                }
            }
        } else {
            // If the text doesn't start with the prefix, return empty config and the original text
            jsonPart = "";
            queryTextPart = text;
        }
        return new String[]{jsonPart, queryTextPart};
    }

    public String getQueryInferenceConfig() {
        return queryInferenceConfig;
    }

    public String getQueryText() {
        return queryText;
    }

    // to be used in tests.
    protected static void reInitializeCompatibilityMode() {
        isCompatibilityModeEnabled = Boolean.getBoolean(EXPERIMENTAL_COMPATIBILITY_MODE_KEY);
    }
}