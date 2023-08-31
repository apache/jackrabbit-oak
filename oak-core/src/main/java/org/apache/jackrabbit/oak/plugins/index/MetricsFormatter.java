package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

public class MetricsFormatter {
    private final JsopBuilder jsopBuilder = new JsopBuilder();
    public static MetricsFormatter newBuilder() {
        return new MetricsFormatter();
    }

    private MetricsFormatter() {
        jsopBuilder.object();
    }

    public MetricsFormatter add(String key, String value) {
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, int value) {
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, long value) {
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, boolean value) {
        jsopBuilder.key(key).value(value);
        return this;
    }

    public String build() {
        jsopBuilder.endObject();
        return jsopBuilder.toString();
    }
}
