/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.run;

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.openFileStore;

import java.io.BufferedWriter;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;

class DumpDataStoreReferencesCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec segmentTar = parser.accepts("segment-tar", "Use oak-segment-tar instead of oak-segment");
        OptionSet options = parser.parse(args);

        if (options.nonOptionArguments().isEmpty()) {
            System.out.println("usage: dumpdatastorerefs {<path>|<mongo-uri>} <dump_path>]");
            System.exit(1);
        }

        Closer closer = Closer.create();
        try {
            BlobReferenceRetriever marker;
            BlobStore blobStore = null;

            String source = options.nonOptionArguments().get(0).toString();

            if (source.startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(source);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore store = new DocumentMK.Builder().setMongoDB(client.getDB(uri.getDatabase())).getNodeStore();
                blobStore = store.getBlobStore();
                closer.register(Utils.asCloseable(store));
                marker = new DocumentBlobReferenceRetriever(store);
            } else if (options.has(segmentTar)) {
                marker = SegmentTarUtils.newBlobReferenceRetriever(source, closer);
            } else {
                FileStore store = openFileStore(source);
                closer.register(Utils.asCloseable(store));
                marker = new SegmentBlobReferenceRetriever(store.getTracker());
            }

            String dumpPath = StandardSystemProperty.JAVA_IO_TMPDIR.value();

            if (options.nonOptionArguments().size() >= 2) {
                dumpPath = options.nonOptionArguments().get(1).toString();
            }

            File dumpFile = new File(dumpPath, "marked-" + System.currentTimeMillis());
            final BufferedWriter writer = Files.newWriter(dumpFile, Charsets.UTF_8);
            final AtomicInteger count = new AtomicInteger();
            try {
                final List<String> idBatch = Lists.newArrayListWithCapacity(1024);
                final Joiner delimJoiner = Joiner.on(",").skipNulls();
                final GarbageCollectableBlobStore gcBlobStore =
                        (blobStore != null && blobStore instanceof GarbageCollectableBlobStore
                                ? (GarbageCollectableBlobStore) blobStore
                                : null);
                marker.collectReferences(
                        new ReferenceCollector() {
                            @Override
                            public void addReference(String blobId, String nodeId) {
                                try {
                                    Iterator<String> idIter = null;
                                    if (gcBlobStore != null) {
                                        idIter = gcBlobStore.resolveChunks(blobId);
                                    } else{
                                        idIter = Iterators.singletonIterator(blobId);
                                    }

                                    while (idIter.hasNext()) {
                                        String id = idIter.next();
                                        idBatch.add(delimJoiner.join(id, nodeId));
                                        count.getAndIncrement();
                                        if (idBatch.size() >= 1024) {
                                            for (String rec : idBatch) {
                                                ExternalSort.writeLine(writer, rec);
                                                writer.append(StandardSystemProperty.LINE_SEPARATOR.value());
                                                writer.flush();
                                            }
                                            idBatch.clear();
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException("Error in retrieving references", e);
                                }
                            }
                        }
                );
                if (!idBatch.isEmpty()) {
                    for (String rec : idBatch) {
                        ExternalSort.writeLine(writer, rec);
                        writer.append(StandardSystemProperty.LINE_SEPARATOR.value());
                        writer.flush();
                    }
                    idBatch.clear();
                }
                System.out.println(count.get() + " DataStore references dumped in " + dumpFile);
            } finally {
                IOUtils.closeQuietly(writer);
            }
        } catch (Throwable t) {
            System.err.println(t.getMessage());
        } finally {
            closer.close();
        }
    }

}
