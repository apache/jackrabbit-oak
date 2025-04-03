package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a specific inference model
 */
public class ModelConfig {
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
        this.similarityThreshold = 0.7f;
        this.minTerms = 2;
    }

    static ModelConfig fromNodeState(NodeState nodeState) {
        ModelConfig config = new ModelConfig();
        
        config.model = getStringProperty(nodeState, "model");
        config.embeddingServiceUrl = getStringProperty(nodeState, "embeddingServiceUrl");
        config.similarityThreshold = getFloatProperty(nodeState, "similarityThreshold", 0.7f);
        config.minTerms = getIntProperty(nodeState, "minTerms", 2);
        config.isDefault = getBooleanProperty(nodeState, "default", false);
        config.enabled = getBooleanProperty(nodeState, "enabled", false);

        NodeState headerNode = nodeState.getChildNode("header");
        for (PropertyState property : headerNode.getProperties()) {
            if (!property.getName().equals("jcr:primaryType")) {
                config.headers.put(property.getName(), property.getValue(Type.STRING));
            }
        }

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