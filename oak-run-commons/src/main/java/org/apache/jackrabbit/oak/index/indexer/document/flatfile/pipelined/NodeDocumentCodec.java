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

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Custom codec to create NodeDocument from a stream of BSON data received from MongoDB.
 * <p>
 * This class is thread-safe.
 */
public class NodeDocumentCodec implements Codec<NodeDocument> {
    // The estimated size is stored in the NodeDocument itself
    public final static String SIZE_FIELD = "_ESTIMATED_SIZE_";

    private final MongoDocumentStore store;
    private final Collection<NodeDocument> collection;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final MongoDocumentFilter nodeDocumentFilter;

    public NodeDocumentCodec(MongoDocumentStore store, Collection<NodeDocument> collection, MongoDocumentFilter documentFilter, CodecRegistry defaultRegistry) {
        this.store = store;
        this.collection = collection;
        this.bsonTypeCodecMap = new BsonTypeCodecMap(new BsonTypeClassMap(), defaultRegistry);
        this.nodeDocumentFilter = documentFilter;
    }

    @Override
    public NodeDocument decode(BsonReader reader, DecoderContext decoderContext) {
        NodeDocument nodeDocument = collection.newDocument(store);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, decoderContext);
            // Once we read the _id or the _path, apply the filter
            if (!nodeDocumentFilter.isFilteringDisabled()
                    && (fieldName.equals(NodeDocument.ID) || fieldName.equals(NodeDocument.PATH))
                    && (value instanceof String)) {
                // value should always be non-null and of type String, but we do not want the filter to ever break the
                // downloader as filtering is best-effort and just a performance optimization. So we check anyway that
                // value is what we expect it to be, and if not, just skip trying to filter.
                if (nodeDocumentFilter.shouldSkip(fieldName, (String) value)) {
                    // The Mongo driver requires us to return a document. To indicate that the document should be skipped,
                    // we return this NULL document. The caller should check for this and skip the document.
                    return NodeDocument.NULL;
                }
            }
            nodeDocument.put(fieldName, value);
        }
        reader.readEndDocument();
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

    private Object readValue(BsonReader reader, String fieldName, DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case STRING:
                return reader.readString();
            case INT64:
                return reader.readInt64();
            case DOCUMENT:
                return readDocument(reader, decoderContext);
            case BOOLEAN:
                return reader.readBoolean();
            case NULL:
                reader.readNull();
                return null;
            case ARRAY:
            case JAVASCRIPT_WITH_SCOPE:
            case DB_POINTER:
            case BINARY:
                throw new UnsupportedOperationException(bsonType.toString());
            default:
                Object value = bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext);
                if (value instanceof Number &&
                        (NodeDocument.MODIFIED_IN_SECS.equals(fieldName) || Document.MOD_COUNT.equals(fieldName))) {
                    value = Utils.asLong((Number) value);
                }
                return value;
        }
    }

    private SortedMap<Revision, Object> readDocument(BsonReader reader, DecoderContext decoderContext) {
        TreeMap<Revision, Object> map = new TreeMap<>(StableRevisionComparator.REVERSE);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, decoderContext);
            map.put(Revision.fromString(fieldName), value);
        }
        reader.readEndDocument();
        return map;
    }
}
