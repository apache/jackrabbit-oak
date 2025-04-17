package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import java.util.List;
import java.util.Map;

public class VectorDocument {

    public static final String ID = "id";
    public static final String VECTOR = "vector";
    public static final String METADATA = "metadata";

    public final String id;

    public final List<Float> vector;

    public final Map<String, Object> metadata;

    public VectorDocument() {
        this.id = null;
        this.vector = null;
        this.metadata = null;
    }

    public VectorDocument(String id, List<Float> vector, Map<String, Object> metadata) {
        this.id = id;
        this.vector = vector;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "VectorDocument{" +
                "id='" + id + '\'' +
                ", vector=" + vector +
                ", metadata=" + metadata +
                '}';
    }
}