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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VectorQueryConfig {
    public static final String TYPE = "inferenceModelConfig";
    @Nullable
    private final String inferenceModelConfig;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public VectorQueryConfig(@NotNull String queryConfig) {
        if (queryConfig.isBlank()){
            this.inferenceModelConfig = null;
        } else if (queryConfig.equals("{}")) {
            // in this case a default inferenceModelConfig will be used.
            this.inferenceModelConfig = "";
        } else {
            try {
                JsonNode jsonNode1 = objectMapper.readTree(queryConfig);
                inferenceModelConfig = jsonNode1.get(TYPE).asText();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error parsing inference query config: "+ queryConfig  + "error message: " + e.getMessage());
            }
        }
    }

    public @Nullable String getInferenceModelConfig() {
        return inferenceModelConfig;
    }
}
