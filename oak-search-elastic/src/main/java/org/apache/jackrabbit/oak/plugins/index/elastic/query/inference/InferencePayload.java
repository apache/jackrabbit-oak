package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Configuration for inference payload
 */
public class InferencePayload {
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