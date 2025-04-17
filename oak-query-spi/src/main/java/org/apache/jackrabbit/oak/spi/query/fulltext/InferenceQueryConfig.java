package org.apache.jackrabbit.oak.spi.query.fulltext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class InferenceQueryConfig {
    public static final String TYPE = "inferenceModelConfig";
    @Nullable
    private final String inferenceModelConfig;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public InferenceQueryConfig(@NotNull String queryConfig) {
        if (queryConfig.isBlank()){
            this.inferenceModelConfig = null;
            return;
        } else if (queryConfig.equals("{}")) {
            // in this case a default inferenceModelConfig will be used.
            this.inferenceModelConfig = "";
        } else {
            try {
                JsonNode jsonNode1 = objectMapper.readTree(queryConfig);
                inferenceModelConfig = jsonNode1.get(TYPE).asText();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error parsing inference query config: "+ queryConfig  + "error message:" + e.getMessage());
            }
        }
    }

    public @Nullable String getInferenceModelConfig() {
        return inferenceModelConfig;
    }
}
