package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.model.AbstractPropertyState;

public class PropertyStateImpl extends AbstractPropertyState {
private final String name;
private final JsonValue value;

public PropertyStateImpl(String name, JsonValue value) {
    this.name = name;
    this.value = value;
}

    public JsonValue getValue() {
        return value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getEncodedValue() {
        return value.toJson();
    }

    @Override
    public String toString() {
        return name + ':' + getEncodedValue();
    }
}
