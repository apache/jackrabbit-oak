package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.Compression;

public class DocumentPropertyStateFactory {

    public static PropertyState createPropertyState(DocumentNodeStore store, String name, String value, Compression compression) {
        if (compression != null && !compression.equals(Compression.NONE)) {
            return new CompressedDocumentPropertyState(store, name, value, compression);
        } else {
            return new DocumentPropertyState(store, name, value);
        }
    }
}
