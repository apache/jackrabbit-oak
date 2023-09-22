package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonTypeCodecMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.TreeMap;

public class NodeDocumentCodec implements Codec<NodeDocument> {
    public final static String SIZE_FIELD = "__SIZE__";
    private final MongoDocumentStore store;
    private final Collection<NodeDocument> col;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final DecoderContext decoderContext = DecoderContext.builder().build();

    private final Codec<String> stringCoded;
    private final Codec<Long> longCoded;
    private final Codec<Boolean> booleanCoded;

    private int size = 0;

    public NodeDocumentCodec(MongoDocumentStore store, Collection<NodeDocument> col, CodecRegistry defaultRegistry) {
        this.store = store;
        this.col = col;
        this.bsonTypeCodecMap = new BsonTypeCodecMap(new BsonTypeClassMap(), defaultRegistry);
        // Retrieve references to the most commonly used codecs, to avoid the map lookup in the common case
        this.stringCoded = (Codec<String>) bsonTypeCodecMap.get(BsonType.STRING);
        this.longCoded = (Codec<Long>) bsonTypeCodecMap.get(BsonType.INT64);
        this.booleanCoded = (Codec<Boolean>) bsonTypeCodecMap.get(BsonType.BOOLEAN);
    }

    @Override
    public NodeDocument decode(BsonReader reader, DecoderContext decoderContext) {
        return convertFromDBObject(store, col, reader);
    }

    @Override
    public void encode(BsonWriter writer, NodeDocument value, EncoderContext encoderContext) {
        throw new UnsupportedOperationException("encode");
    }

    @Override
    public Class<NodeDocument> getEncoderClass() {
        return NodeDocument.class;
    }

    private NodeDocument convertFromDBObject(MongoDocumentStore store, Collection<NodeDocument> collection, BsonReader reader) {
        NodeDocument nodeDocument = collection.newDocument(store);
        size = 0;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName);
            nodeDocument.put(fieldName, value);
        }
        reader.readEndDocument();
        nodeDocument.put(SIZE_FIELD, size);
        return nodeDocument;
    }


    private Object readValue(BsonReader reader, String fieldName) {
        size += 16 + fieldName.length();
        BsonType bsonType = reader.getCurrentBsonType();
        Object value;
        int valSize;
        switch (bsonType) {
            case STRING:
                String sValue = stringCoded.decode(reader, decoderContext);
                valSize = 16 + sValue.length() * 2;
                value = sValue;
                break;
            case INT64:
                value = longCoded.decode(reader, decoderContext);
                valSize = 16;
                break;
            case DOCUMENT:
                value = convertMongoMap(reader);
                valSize = 0; // the size is updated by the recursive calls done inside the method above
                break;
            case BOOLEAN:
                value = booleanCoded.decode(reader, decoderContext);
                valSize = 16;
                break;
            case NULL:
                reader.readNull();
                value = null;
                valSize = 0;
                break;
            case ARRAY:
                throw new UnsupportedOperationException("ARRAY");
            case JAVASCRIPT_WITH_SCOPE:
                throw new UnsupportedOperationException("JAVASCRIPT_WITH_SCOPE");
            case DB_POINTER:
                throw new UnsupportedOperationException("DB_POINTER");
            case BINARY:
                throw new UnsupportedOperationException("BINARY");
            default:
                value = bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext);
                valSize = 16;
                if (value instanceof Number &&
                        (NodeDocument.MODIFIED_IN_SECS.equals(fieldName) || Document.MOD_COUNT.equals(fieldName))) {
                    value = Utils.asLong((Number) value);
                }
                break;
        }
        size += valSize;
        return value;
    }

    private Map<Revision, Object> convertMongoMap(BsonReader reader) {
        TreeMap<Revision, Object> map = new TreeMap<>(StableRevisionComparator.REVERSE);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName);
            map.put(Revision.fromString(fieldName), value);
        }
        reader.readEndDocument();
        return map;
    }
}
