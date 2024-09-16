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

import java.util.List;
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

    public static final String OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_FILTERED_PATH = "oak.indexer.pipelined.nodeDocument.filter.filteredPath";
    public static final String OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP = "oak.indexer.pipelined.nodeDocument.filter.suffixesToSkip";
    private final String filteredPath = ConfigHelper.getSystemPropertyAsString(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_FILTERED_PATH, "");
    private final List<String> suffixesToSkip = ConfigHelper.getSystemPropertyAsStringList(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "", ';');

    // The estimated size is stored in the NodeDocument itself
    public final static String SIZE_FIELD = "_ESTIMATED_SIZE_";

    private static class NodeDocumentDecoderContext {
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

    private final NodeDocumentFilter nodeDocumentFilter = new NodeDocumentFilter(filteredPath, suffixesToSkip);

    // Statistics
    private final AtomicLong totalDocsDecoded = new AtomicLong(0);
    private final AtomicLong totalDataDownloaded = new AtomicLong(0);
    private final ThreadLocal<NodeDocumentDecoderContext> perThreadContext = ThreadLocal.withInitial(NodeDocumentDecoderContext::new);

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

    /**
     * Skipping over values in the BSON file is faster than reading them. Skipping is done by advancing a pointer in
     * an internal buffer, while reading requires converting them to a Java data type (typically String).
     */
    private void skipUntilEndOfDocument(BsonReader reader) {
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            reader.skipName();
            reader.skipValue();
        }
        reader.readEndDocument();
    }

    @Override
    public NodeDocument decode(BsonReader reader, DecoderContext decoderContext) {
        NodeDocument nodeDocument = collection.newDocument(store);
        NodeDocumentDecoderContext threadLocalContext = perThreadContext.get();
        threadLocalContext.estimatedSizeOfCurrentObject = 0;
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, threadLocalContext);
            // Once we read the _id or the _path, apply the filter
            if (!nodeDocumentFilter.isFilteringDisabled()
                    && (fieldName.equals(NodeDocument.ID) || fieldName.equals(NodeDocument.PATH))
                    && (value instanceof String)) {
                // value should always be non-null and of type String, but we do not want the filter to ever break the
                // downlaoder as filtering is best-effort and just a performance optimization. So we check anyway that
                // value is what we expect it to be, and if not, just skip trying to filter.
                if (nodeDocumentFilter.shouldSkip(fieldName, (String) value)) {
                    skipUntilEndOfDocument(reader);
                    // The Mongo driver requires us to return a document. To indicate that the document should be skipped,
                    // we return an empty document. The logic reading from the Mongo cursor can then check if the _id of
                    // the document is null, which indicates that the document should be skipped.
                    return emptyNodeDocument;
                }
            }
            nodeDocument.put(fieldName, value);
        }
        reader.readEndDocument();
        threadLocalContext.docsDecoded++;
        threadLocalContext.dataDownloaded += threadLocalContext.estimatedSizeOfCurrentObject;
        long docsDecodedLocal = totalDocsDecoded.incrementAndGet();
        long dataDownloadedLocal = totalDataDownloaded.addAndGet(threadLocalContext.estimatedSizeOfCurrentObject);
        if (docsDecodedLocal % 500_000 == 0) {
            ConcurrentHashMap<String, MutableLong> filteredSuffixes = nodeDocumentFilter.getFilteredSuffixesCounts();
            long totalDocumentsFiltered = filteredSuffixes.values().stream().mapToLong(MutableLong::longValue).sum();
            String filteredRenditionsString = filteredSuffixes.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                    .limit(10)
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(", ", "{", "}"));
            LOG.info("docsDecodedByThread: {}, dataDownloadedByThread: {}, docsDecodedTotal: {}, dataDownloadedTotal: {}, docsSkippedTotal {}, filteredRenditionsTotal (top 10): {}",
                    threadLocalContext.docsDecoded, IOUtils.humanReadableByteCountBin(threadLocalContext.dataDownloaded),
                    totalDocsDecoded, IOUtils.humanReadableByteCountBin(dataDownloadedLocal),
                    totalDocumentsFiltered, filteredRenditionsString);
        }
        nodeDocument.put(SIZE_FIELD, threadLocalContext.estimatedSizeOfCurrentObject);
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

    private Object readValue(BsonReader reader, String fieldName, NodeDocumentDecoderContext threadContext) {
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
                value = readDocument(reader, threadContext);
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
        threadContext.estimatedSizeOfCurrentObject += 16 + fieldName.length() + valSize;
        return value;
    }

    private SortedMap<Revision, Object> readDocument(BsonReader reader, NodeDocumentDecoderContext threadContext) {
        TreeMap<Revision, Object> map = new TreeMap<>(StableRevisionComparator.REVERSE);
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Object value = readValue(reader, fieldName, threadContext);
            map.put(Revision.fromString(fieldName), value);
        }
        reader.readEndDocument();
        return map;
    }
}
