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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.jackrabbit.oak.commons.IOUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
 * <p>
 * This class must be thread-safe, Mongo uses a single coded implementation across multiple threads.
 */
public class NodeDocumentCodec implements Codec<NodeDocument> {
    private final static Logger LOG = LoggerFactory.getLogger(NodeDocumentCodec.class);

    // The estimated size is stored in the NodeDocument itself
    public final static String SIZE_FIELD = "_ESTIMATED_SIZE_";

    private static class StatsDecoderContext {
        long docsDecoded = 0;
        long dataDownloaded = 0;
        int estimatedSizeOfCurrentObject = 0;
    }

    private final NodeDocument emptyNodeDocument;

    private final MongoDocumentStore store;
    private final Collection<NodeDocument> collection;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final DecoderContext decoderContext = DecoderContext.builder().build();
    private final Codec<String> stringCoded;
    private final Codec<Long> longCoded;
    private final Codec<Boolean> booleanCoded;

    private final NodeDocumentFilter fieldFilter = new NodeDocumentFilter();

    // Statistics
    private final AtomicLong totalDocsDecoded = new AtomicLong(0);
    private final AtomicLong totalDataDownloaded = new AtomicLong(0);
    private final ThreadLocal<StatsDecoderContext> perThreadContext = ThreadLocal.withInitial(StatsDecoderContext::new);

    public NodeDocumentCodec(MongoDocumentStore store, Collection<NodeDocument> collection, CodecRegistry defaultRegistry) {
        this.store = store;
        this.collection = collection;
        this.bsonTypeCodecMap = new BsonTypeCodecMap(new BsonTypeClassMap(), defaultRegistry);
        this.emptyNodeDocument = collection.newDocument(store);
        // Retrieve references to the most commonly used codecs, to avoid the map lookup in the common case
        this.stringCoded = (Codec<String>) bsonTypeCodecMap.get(BsonType.STRING);
        this.longCoded = (Codec<Long>) bsonTypeCodecMap.get(BsonType.INT64);
        this.booleanCoded = (Codec<Boolean>) bsonTypeCodecMap.get(BsonType.BOOLEAN);
    }

    private void skipUntilEndOfDocument(BsonReader reader) {
        BsonType bsonType = reader.readBsonType();
        while (bsonType != BsonType.END_OF_DOCUMENT) {
            reader.skipName();
            reader.skipValue();
            bsonType = reader.readBsonType();
        }
        reader.readEndDocument();
    }

    @Override
    public NodeDocument decode(BsonReader reader, DecoderContext decoderContext) {
        NodeDocument nodeDocument = collection.newDocument(store);
        StatsDecoderContext statsContext = perThreadContext.get();
        statsContext.estimatedSizeOfCurrentObject = 0;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, statsContext);
            if (fieldName.equals(NodeDocument.ID) || fieldName.equals(NodeDocument.PATH)) {
                if (fieldFilter.shouldSkip(fieldName, (String) value)) {
                    skipUntilEndOfDocument(reader);
                    return emptyNodeDocument;
                }
            }
            nodeDocument.put(fieldName, value);
        }
        reader.readEndDocument();
        statsContext.docsDecoded++;
        statsContext.dataDownloaded += statsContext.estimatedSizeOfCurrentObject;
        long docsDecodedLocal = totalDocsDecoded.incrementAndGet();
        long dataDownloadedLocal = totalDataDownloaded.addAndGet(statsContext.estimatedSizeOfCurrentObject);
        if (docsDecodedLocal % 200_000 == 0) {
            ConcurrentHashMap<String, MutableLong> filteredSuffixes = fieldFilter.getFilteredSuffixesCounts();
            long totalDocumentsFiltered = filteredSuffixes.values().stream().mapToLong(MutableLong::longValue).sum();
            String filteredRenditionsString = filteredSuffixes.entrySet().stream()
                    .sorted((e1, e2) -> -e2.getValue().compareTo(e1.getValue()))
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(", ", "{", "}"));
            LOG.info("docsDecoded: {}, dataDownloaded: {}, totalDocsDecoded: {}, totalDataDownloaded: {}, docsSkipped {}, filteredRenditions: {}",
                    statsContext.docsDecoded, IOUtils.humanReadableByteCountBin(statsContext.dataDownloaded),
                    totalDocsDecoded, IOUtils.humanReadableByteCountBin(dataDownloadedLocal),
                    totalDocumentsFiltered, filteredRenditionsString);
        }
        nodeDocument.put(SIZE_FIELD, statsContext.estimatedSizeOfCurrentObject);
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

    private Object readValue(BsonReader reader, String fieldName, StatsDecoderContext statsContext) {
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
                value = readDocument(reader, statsContext);
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
            case JAVASCRIPT_WITH_SCOPE:
            case DB_POINTER:
            case BINARY:
                throw new UnsupportedOperationException(bsonType.toString());
            default:
                value = bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext);
                valSize = 16;
                if (value instanceof Number &&
                        (NodeDocument.MODIFIED_IN_SECS.equals(fieldName) || Document.MOD_COUNT.equals(fieldName))) {
                    value = Utils.asLong((Number) value);
                }
                break;
        }
        statsContext.estimatedSizeOfCurrentObject += 16 + fieldName.length() + valSize;
        return value;
    }

    private SortedMap<Revision, Object> readDocument(BsonReader reader, StatsDecoderContext statsContext) {
        TreeMap<Revision, Object> map = new TreeMap<>(StableRevisionComparator.REVERSE);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, statsContext);
            map.put(Revision.fromString(fieldName), value);
        }
        reader.readEndDocument();
        return map;
    }
}
