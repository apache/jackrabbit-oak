package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

// This response object maps to open-ai embeddings api response
public class InferenceResponseModel {

    private String object;
    private List<EmbeddingData> data;
    private String model;
    private Usage usage;

    public static class EmbeddingData {
        private String object;
        private List<Float> embedding;
        private int index;

        public String getObject() {
            return object;
        }

        public void setObject(String object) {
            this.object = object;
        }

        public List<Float> getEmbedding() {
            return embedding;
        }

        public void setEmbedding(List<Float> embedding) {
            this.embedding = embedding;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

    public static class Usage {
        
        @JsonProperty("prompt_tokens")
        private int promptTokens;
        @JsonProperty("total_tokens") 
        private int totalTokens;

        public int getPromptTokens() {
            return promptTokens;
        }

        public void setPromptTokens(int promptTokens) {
            this.promptTokens = promptTokens;
        }

        public int getTotalTokens() {
            return totalTokens;
        }

        public void setTotalTokens(int totalTokens) {
            this.totalTokens = totalTokens;
        }
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public List<EmbeddingData> getData() {
        return data;
    }

    public void setData(List<EmbeddingData> data) {
        this.data = data;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Usage getUsage() {
        return usage;
    }

    public void setUsage(Usage usage) {
        this.usage = usage;
    }

}
