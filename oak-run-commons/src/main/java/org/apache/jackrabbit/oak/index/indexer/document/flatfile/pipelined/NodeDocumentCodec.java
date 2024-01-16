/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

/**
 * Custom codec for MongoDB to transform a stream of BSON tokens into a NodeDocument. This custom codec provides two
 * benefits compared to using a standard Mongo codec.
 * <ul>
 *   <li>The standard codecs produce objects from the Mongo client API (BasicDBObject or BsonDocument or Document) which have
 *   then to be converted into NodeDocuments (OAK API). This custom codec creates directly a NodeDocument, thereby
 *   skipping the intermediate object. This should be more efficient and reduce the pressure on the GC.</li>
 *   <li>Allows estimating the size of the document while reading it, which will have a negligible overhead (as compared
 *   with doing an additional traverse of the object structure to compute the size).</li>
 * </ul>
 */
public class NodeDocumentCodec implements Codec<NodeDocument> {
    // The estimated size is stored in the NodeDocument itself
    public final static String SIZE_FIELD = "__ESTIMATED_SIZE__";
    private final MongoDocumentStore store;
    private final Collection<NodeDocument> collection;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final DecoderContext decoderContext = DecoderContext.builder().build();

    private final Codec<String> stringCoded;
    private final Codec<Long> longCoded;
    private final Codec<Boolean> booleanCoded;

    private int estimatedSizeOfCurrentObject = 0;

    private final FieldSizeTracker fieldSizeTracker = new FieldSizeTracker.HashMapFieldSizeTracker();

    public NodeDocumentCodec(MongoDocumentStore store, Collection<NodeDocument> collection, CodecRegistry defaultRegistry) {
        this.store = store;
        this.collection = collection;
        this.bsonTypeCodecMap = new BsonTypeCodecMap(new BsonTypeClassMap(), defaultRegistry);
        // Retrieve references to the most commonly used codecs, to avoid the map lookup in the common case
        this.stringCoded = (Codec<String>) bsonTypeCodecMap.get(BsonType.STRING);
        this.longCoded = (Codec<Long>) bsonTypeCodecMap.get(BsonType.INT64);
        this.booleanCoded = (Codec<Boolean>) bsonTypeCodecMap.get(BsonType.BOOLEAN);
    }

    @Override
    public NodeDocument decode(BsonReader reader, DecoderContext decoderContext) {
        NodeDocument nodeDocument = collection.newDocument(store);
        estimatedSizeOfCurrentObject = 0;
        int prevEstimatedSizeOfCurrentObject = 0;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName);
            nodeDocument.put(fieldName, value);
            fieldSizeTracker.addField(fieldName, estimatedSizeOfCurrentObject- prevEstimatedSizeOfCurrentObject);
            prevEstimatedSizeOfCurrentObject = estimatedSizeOfCurrentObject;
        }
        reader.readEndDocument();
        nodeDocument.put(SIZE_FIELD, estimatedSizeOfCurrentObject);
        return nodeDocument;
    }

    @Override
    public void encode(BsonWriter writer, NodeDocument value, EncoderContext encoderContext) {
        throw new UnsupportedOperationException("encode");
    }

    @Override
    public Class<NodeDocument> getEncoderClass() {
        return NodeDocument.class;
    }

    private Object readValue(BsonReader reader, String fieldName) {
        estimatedSizeOfCurrentObject += 16 + fieldName.length();
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
                value = readDocument(reader);
                valSize = 0; // the size is updated by the recursive calls inside readDocument
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
        estimatedSizeOfCurrentObject += valSize;
        return value;
    }

    private Map<Revision, Object> readDocument(BsonReader reader) {
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

    public FieldSizeTracker getFieldSizeTracker() {
        return fieldSizeTracker;
    }
}
