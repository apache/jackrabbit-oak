package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.HashMap;
import java.util.Map;

/**
 * Data model class representing the inference configuration stored under /oak:index/:inferenceConfig
 */
public class InferenceConfiguration {
    private boolean enabled;
    private Map<String, IndexInferenceConfig> indexConfigs;

    public InferenceConfiguration() {
        this.indexConfigs = new HashMap<>();
    }

    /**
     * Loads configuration from the given NodeState
     * @param nodeState NodeState representing :inferenceConfig node
     * @return InferenceConfiguration instance
     */
    public static InferenceConfiguration fromNodeState(NodeState nodeState) {
        InferenceConfiguration config = new InferenceConfiguration();
        
        // Read enabled flag
        PropertyState enabledProp = nodeState.getProperty("enabled");
        config.enabled = enabledProp != null && enabledProp.getValue(Type.BOOLEAN);

        // Read index configurations
        for (String indexName : nodeState.getChildNodeNames()) {
            if (!"jcr:primaryType".equals(indexName)) {
                NodeState indexNode = nodeState.getChildNode(indexName);
                config.indexConfigs.put(indexName, IndexInferenceConfig.fromNodeState(indexNode));
            }
        }

        return config;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Map<String, IndexInferenceConfig> getIndexConfigs() {
        return indexConfigs;
    }

    /**
     * Inner class representing configuration for a specific index
     */
    public static class IndexInferenceConfig {
        private String enricherConfig;
        private Map<String, ModelConfig> modelConfigs;

        public IndexInferenceConfig() {
            this.modelConfigs = new HashMap<>();
        }

        static IndexInferenceConfig fromNodeState(NodeState nodeState) {
            IndexInferenceConfig config = new IndexInferenceConfig();
            
            // Read enricherConfig
            PropertyState enricherConfigProp = nodeState.getProperty("enricherConfig");
            if (enricherConfigProp != null) {
                config.enricherConfig = enricherConfigProp.getValue(Type.STRING);
            }

            // Read model configurations
            for (String configName : nodeState.getChildNodeNames()) {
                if (!"jcr:primaryType".equals(configName) && !"enricherConfig".equals(configName)) {
                    NodeState modelNode = nodeState.getChildNode(configName);
                    config.modelConfigs.put(configName, ModelConfig.fromNodeState(modelNode));
                }
            }

            return config;
        }

        public String getEnricherConfig() {
            return enricherConfig;
        }

        public Map<String, ModelConfig> getModelConfigs() {
            return modelConfigs;
        }
    }

    /**
     * Inner class representing configuration for a specific inference model
     */
    public static class ModelConfig {
        private String model;
        private String embeddingServiceUrl;
        private float similarityThreshold;
        private int minTerms;
        private boolean isDefault;
        private boolean enabled;
        private Map<String, String> headers;
        private InferencePayload inferencePayload;

        public ModelConfig() {
            this.headers = new HashMap<>();
            this.similarityThreshold = 0.7f; // default value
            this.minTerms = 2; // default value
        }

        static ModelConfig fromNodeState(NodeState nodeState) {
            ModelConfig config = new ModelConfig();
            
            // Read basic properties
            config.model = getStringProperty(nodeState, "model");
            config.embeddingServiceUrl = getStringProperty(nodeState, "embeddingServiceUrl");
            config.similarityThreshold = getFloatProperty(nodeState, "similarityThreshold", 0.7f);
            config.minTerms = getIntProperty(nodeState, "minTerms", 2);
            config.isDefault = getBooleanProperty(nodeState, "default", false);
            config.enabled = getBooleanProperty(nodeState, "enabled", false);

            // Read headers
            NodeState headerNode = nodeState.getChildNode("header");
            for (PropertyState property : headerNode.getProperties()) {
                if (!property.getName().equals("jcr:primaryType")) {
                    config.headers.put(property.getName(), property.getValue(Type.STRING));
                }
            }

            // Read inference payload
            NodeState payloadNode = nodeState.getChildNode("inferencepayload");
            if (payloadNode.exists()) {
                config.inferencePayload = InferencePayload.fromNodeState(payloadNode);
            }

            return config;
        }

        private static String getStringProperty(NodeState node, String name) {
            PropertyState prop = node.getProperty(name);
            return prop != null ? prop.getValue(Type.STRING) : null;
        }

        private static float getFloatProperty(NodeState node, String name, float defaultValue) {
            PropertyState prop = node.getProperty(name);
            return prop != null ? Float.parseFloat(prop.getValue(Type.STRING)) : defaultValue;
        }

        private static int getIntProperty(NodeState node, String name, int defaultValue) {
            PropertyState prop = node.getProperty(name);
            return prop != null ? Integer.parseInt(prop.getValue(Type.STRING)) : defaultValue;
        }

        private static boolean getBooleanProperty(NodeState node, String name, boolean defaultValue) {
            PropertyState prop = node.getProperty(name);
            return prop != null ? prop.getValue(Type.BOOLEAN) : defaultValue;
        }

        // Getters
        public String getModel() { return model; }
        public String getEmbeddingServiceUrl() { return embeddingServiceUrl; }
        public float getSimilarityThreshold() { return similarityThreshold; }
        public int getMinTerms() { return minTerms; }
        public boolean isDefault() { return isDefault; }
        public boolean isEnabled() { return enabled; }
        public Map<String, String> getHeaders() { return headers; }
        public InferencePayload getInferencePayload() { return inferencePayload; }
    }

    /**
     * Inner class representing inference payload configuration
     */
    public static class InferencePayload {
        private String textKey;
        private String dimension;
        private String model;

        static InferencePayload fromNodeState(NodeState nodeState) {
            InferencePayload payload = new InferencePayload();
            
            payload.textKey = getStringProperty(nodeState, "textKey");
            payload.dimension = getStringProperty(nodeState, "dimension");
            payload.model = getStringProperty(nodeState, "model");

            return payload;
        }

        private static String getStringProperty(NodeState node, String name) {
            PropertyState prop = node.getProperty(name);
            return prop != null ? prop.getValue(Type.STRING) : null;
        }

        // Getters
        public String getTextKey() { return textKey; }
        public String getDimension() { return dimension; }
        public String getModel() { return model; }
    }
} 