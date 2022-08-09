package org.apache.jackrabbit.oak.util;

import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

import java.util.Map;

public class TestFeatureToggleFactory {

    static class FeatureFactoryWhiteBoard extends DefaultWhiteboard {
        final private boolean enabled;

        FeatureFactoryWhiteBoard(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
            if (service instanceof FeatureToggle) {
                ((FeatureToggle) service).setEnabled(enabled);
            }
            return super.register(type, service, properties);
        }
    }

    public static Feature newFeature(String name, boolean enabled) {
        return Feature.newFeature(name, new FeatureFactoryWhiteBoard(enabled));
    }
}
