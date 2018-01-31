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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

/**
 * Helper class to access package private method of DocumentNodeStore and other
 * classes in this package.
 */
public class DocumentNodeStoreHelper {

    private DocumentNodeStoreHelper() {
    }

    public static Cache<PathRev, DocumentNodeState> getNodesCache(DocumentNodeStore dns) {
        return dns.getNodeCache();
    }

    public static void garbageReport(DocumentNodeStore dns) {
        System.out.print("Collecting top 100 nodes with most blob garbage ");
        Stopwatch sw = Stopwatch.createStarted();
        Iterable<BlobReferences> refs = scan(dns, new BlobGarbageSizeComparator(), 100);
        for (BlobReferences br : refs) {
            System.out.println(br);
        }
        System.out.println("Collected in " + sw.stop());
    }

    public static VersionGarbageCollector createVersionGC(
            DocumentNodeStore nodeStore, VersionGCSupport gcSupport) {
        return new VersionGarbageCollector(nodeStore, gcSupport);
    }

    private static Iterable<BlobReferences> scan(DocumentNodeStore store,
                                                 Comparator<BlobReferences> comparator,
                                                 int num) {
        long totalGarbage = 0;
        Iterable<NodeDocument> docs = getDocuments(store.getDocumentStore());
        PriorityQueue<BlobReferences> queue = new PriorityQueue<BlobReferences>(num, comparator);
        List<Blob> blobs = Lists.newArrayList();
        long docCount = 0;
        for (NodeDocument doc : docs) {
            if (++docCount % 10000 == 0) {
                System.out.print(".");
            }
            blobs.clear();
            BlobReferences refs = collectReferences(doc, store);
            totalGarbage += refs.garbageSize;
            queue.add(refs);
            if (queue.size() > num) {
                queue.remove();
            }
        }

        System.out.println();
        List<BlobReferences> refs = Lists.newArrayList();
        refs.addAll(queue);
        Collections.sort(refs, Collections.reverseOrder(comparator));
        System.out.println("Total garbage size: " + FileUtils.byteCountToDisplaySize(totalGarbage));
        System.out.println("Total number of nodes with blob references: " + docCount);
        System.out.println("total referenced / old referenced / # blob references / path");
        return refs;
    }

    private static BlobReferences collectReferences(NodeDocument doc,
                                                    DocumentNodeStore ns) {
        long blobSize = 0;
        long garbageSize = 0;
        int numBlobs = 0;

        List<Blob> blobs = Lists.newArrayList();
        RevisionVector head = ns.getHeadRevision();
        boolean exists = doc.getNodeAtRevision(ns, head, null) != null;
        for (String key : doc.keySet()) {
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            boolean foundValid = false;
            Map<Revision, String> valueMap = doc.getLocalMap(key);
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                blobs.clear();
                String v = entry.getValue();
                if (v != null) {
                    loadValue(v, blobs, ns);
                }
                blobSize += size(blobs);
                if (foundValid) {
                    garbageSize += size(blobs);
                } else if (Utils.isCommitted(ns.getCommitValue(entry.getKey(), doc))) {
                    foundValid = true;
                } else {
                    garbageSize += size(blobs);
                }
                numBlobs += blobs.size();
            }
        }
        return new BlobReferences(doc.getPath(), blobSize, numBlobs, garbageSize, exists);
    }

    private static Iterable<NodeDocument> getDocuments(DocumentStore store) {
        if (store instanceof MongoDocumentStore) {
            // optimized implementation for MongoDocumentStore
            final MongoDocumentStore mds = (MongoDocumentStore) store;
            DBCollection dbCol = MongoDocumentStoreHelper.getDBCollection(
                    mds, Collection.NODES);
            DBObject query = QueryBuilder.start(NodeDocument.HAS_BINARY_FLAG)
                    .is(NodeDocument.HAS_BINARY_VAL)
                    .get();
            DBCursor cursor = dbCol.find(query);
            return Iterables.transform(cursor, new Function<DBObject, NodeDocument>() {
                @Nullable
                @Override
                public NodeDocument apply(DBObject input) {
                    return MongoDocumentStoreHelper.convertFromDBObject(mds, Collection.NODES, input);
                }
            });
        } else {
            return Utils.getSelectedDocuments(store,
                    NodeDocument.HAS_BINARY_FLAG, NodeDocument.HAS_BINARY_VAL);
        }
    }


    private static long size(Iterable<Blob> blobs) {
        long size = 0;
        for (Blob b : blobs) {
            size += b.length();
        }
        return size;
    }

    private static void loadValue(String v, java.util.Collection<Blob> blobs,
                                  DocumentNodeStore nodeStore) {
        JsopReader reader = new JsopTokenizer(v);
        PropertyState p;
        if (reader.matches('[')) {
            p = DocumentPropertyState.readArrayProperty("x", nodeStore, reader);
            if (p.getType() == Type.BINARIES) {
                for (int i = 0; i < p.count(); i++) {
                    Blob b = p.getValue(Type.BINARY, i);
                    blobs.add(b);
                }
            }
        } else {
            p = DocumentPropertyState.readProperty("x", nodeStore, reader);
            if (p.getType() == Type.BINARY) {
                Blob b = p.getValue(Type.BINARY);
                blobs.add(b);
            }
        }
    }

    private static class BlobReferences {

        final String path;
        final long blobSize;
        final long garbageSize;
        final int numBlobs;
        final boolean exists;

        public BlobReferences(String path,
                              long blobSize,
                              int numBlobs,
                              long garbageSize,
                              boolean exists) {
            this.path = path;
            this.blobSize = blobSize;
            this.garbageSize = garbageSize;
            this.numBlobs = numBlobs;
            this.exists = exists;
        }

        @Override
        public String toString() {
            String s = FileUtils.byteCountToDisplaySize(blobSize) + "\t"
                    + FileUtils.byteCountToDisplaySize(garbageSize) + "\t"
                    + numBlobs + "\t"
                    + path;
            if (!exists) {
                s += "\t(deleted)";
            }
            return s;
        }
    }

    private static class BlobGarbageSizeComparator
            implements Comparator<BlobReferences> {

        @Override
        public int compare(BlobReferences o1, BlobReferences o2) {
            int c = Longs.compare(o1.garbageSize, o2.garbageSize);
            if (c != 0) {
                return c;
            }
            return o1.path.compareTo(o2.path);
        }
    }
}
