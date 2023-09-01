package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

public class MetricsFormatter {
    private final JsopBuilder jsopBuilder = new JsopBuilder();
    private boolean isWritable = true;
    public static MetricsFormatter newBuilder() {
        return new MetricsFormatter();
    }

    private MetricsFormatter() {
        jsopBuilder.object();
    }

    public MetricsFormatter add(String key, String value) {
        Preconditions.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, int value) {
        Preconditions.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, long value) {
        Preconditions.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, boolean value) {
        Preconditions.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public String build() {
        if (isWritable){
            jsopBuilder.endObject();
            isWritable = false;
        }
        return jsopBuilder.toString();
    }
}
