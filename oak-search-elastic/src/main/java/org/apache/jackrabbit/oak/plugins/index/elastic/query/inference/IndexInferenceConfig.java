package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a specific index
 */
public class IndexInferenceConfig {
    private String enricherConfig;
    private Map<String, ModelConfig> modelConfigs;

    public IndexInferenceConfig() {
        this.modelConfigs = new HashMap<>();
    }

    static IndexInferenceConfig fromNodeState(NodeState nodeState) {
        IndexInferenceConfig config = new IndexInferenceConfig();
        
        PropertyState enricherConfigProp = nodeState.getProperty("enricherConfig");
        if (enricherConfigProp != null) {
            config.enricherConfig = enricherConfigProp.getValue(Type.STRING);
        }

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