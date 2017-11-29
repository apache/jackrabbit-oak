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

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.io.Closeables.close;
import static java.io.File.createTempFile;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeAsLine;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils.FileLineDifferenceIterator;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Command to check data store consistency and also optionally retrieve ids
 * and references.
 */
public class DataStoreCheckCommand implements Command {
    private static final String DELIM = ",";

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();

        String helpStr =
            "datastorecheck [--id] [--ref] [--consistency] [--store <path>|<mongo_uri>] "
                + "[--s3ds <s3ds_config>|--fds <fds_config>|--azureblobds <azureblobds_config>]"
                + " [--dump <path>] [--repoHome <repo_home>] [--track]";

        Closer closer = Closer.create();
        try {
            // Options for operations requested
            OptionSpecBuilder idOp = parser.accepts("id", "Get ids");
            OptionSpecBuilder refOp = parser.accepts("ref", "Get references");
            OptionSpecBuilder consistencyOp = parser.accepts("consistency", "Check consistency");

            // Node Store - needed for --ref, --consistency
            ArgumentAcceptingOptionSpec<String> store = parser.accepts("store", "Node Store")
                .requiredIf(refOp, consistencyOp).withRequiredArg().ofType(String.class);
            // Optional argument to specify the dump path
            ArgumentAcceptingOptionSpec<String> dump = parser.accepts("dump", "Dump Path")
                .withRequiredArg().ofType(String.class);

            // Optional argument to specify tracking
            OptionSpecBuilder trackOverride = parser.accepts("track", "Force override tracked ids");

            // Required argument for --consistency to specify tracking folder (otherwise can have inconsistencies)
            ArgumentAcceptingOptionSpec<String> repoHome = parser.accepts("repoHome", "Local repository home folder")
                .requiredIf(trackOverride, consistencyOp).withRequiredArg().ofType(String.class);


            OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                "show help").forHelp();

            // Required rules (any one of --id, --ref, --consistency)
            idOp.requiredUnless(refOp, consistencyOp);
            refOp.requiredUnless(idOp, consistencyOp);
            consistencyOp.requiredUnless(idOp, refOp);
            trackOverride.availableIf(idOp, consistencyOp);

            OptionSet options = null;
            try {
                options = parser.parse(args);
            } catch (Exception e) {
                System.err.println(e);
                System.err.println(Arrays.toString(args));
                System.err.println();
                System.err.println("Options :");
                parser.printHelpOn(System.err);
                return;
            }

            if (options.has(help)) {
                parser.printHelpOn(System.out);
                return;
            }

            String dumpPath = JAVA_IO_TMPDIR.value();
            if (options.has(dump)) {
                dumpPath = options.valueOf(dump);
            }

            GarbageCollectableBlobStore blobStore  = null;
            BlobReferenceRetriever marker = null;
            if (options.has(store)) {
                String source = options.valueOf(store);
                if (source.startsWith(MongoURI.MONGODB_PREFIX)) {
                    MongoClientURI uri = new MongoClientURI(source);
                    MongoClient client = new MongoClient(uri);
                    DocumentNodeStore nodeStore =
                        new DocumentMK.Builder().setMongoDB(client.getDB(uri.getDatabase())).getNodeStore();
                    closer.register(Utils.asCloseable(nodeStore));
                    blobStore = (GarbageCollectableBlobStore) nodeStore.getBlobStore();
                    marker = new DocumentBlobReferenceRetriever(nodeStore);
                } else {
                    marker = SegmentTarUtils.newBlobReferenceRetriever(source, closer);
                }
            }

            // Initialize S3/FileDataStore if configured
            GarbageCollectableBlobStore dataStore  = Utils.bootstrapDataStore(args, closer);
            if (dataStore != null) {
                blobStore = dataStore;
            }

            // blob store still not initialized means configuration not supported
            if (blobStore == null) {
                System.err.println("Operation not defined for SegmentNodeStore without external datastore");
                parser.printHelpOn(System.err);
                return;
            }

            FileRegister register = new FileRegister(options);
            closer.register(register);

            if (options.has(idOp) || options.has(consistencyOp)) {
                File dumpFile = register.createFile(idOp, dumpPath);
                retrieveBlobIds(blobStore, dumpFile);

                // If track path and track override specified copy the file to the location
                if (options.has(repoHome) && options.has(trackOverride)) {
                    String trackPath = options.valueOf(repoHome);
                    File trackingFileParent = new File(FilenameUtils.concat(trackPath, "blobids"));
                    File trackingFile = new File(trackingFileParent,
                        "blob-" + String.valueOf(System.currentTimeMillis()) + ".gen");
                    FileUtils.copyFile(dumpFile, trackingFile);
                }
            }

            if (options.has(refOp) || options.has(consistencyOp)) {
                retrieveBlobReferences(blobStore, marker,
                    register.createFile(refOp, dumpPath));
            }

            if (options.has(consistencyOp)) {
                checkConsistency(register.get(idOp), register.get(refOp),
                    register.createFile(consistencyOp, dumpPath), options.valueOf(repoHome));
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            closer.close();
        }
    }

    static class FileRegister implements Closeable {
        Map<OptionSpec, File> opFiles = Maps.newHashMap();
        String suffix = String.valueOf(System.currentTimeMillis());
        OptionSet options;

        public FileRegister(OptionSet options) {
            this.options = options;
        }

        public File createFile(OptionSpec spec, String path) {
            File f = new File(path, spec.toString() + suffix);
            opFiles.put(spec, f);
            return f;
        }

        public File get(OptionSpec spec) {
            return opFiles.get(spec);
        }

        @Override
        public void close() throws IOException {
            Iterator<Map.Entry<OptionSpec, File>> iterator = opFiles.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<OptionSpec, File> entry = iterator.next();
                File f = entry.getValue();
                if (options.has(entry.getKey()) && f.length() != 0) {
                    System.out.println(entry.getKey().toString() + " - " + f.getAbsolutePath());
                } else {
                    if (f.exists()) {
                        forceDelete(f);
                    }
                }
            }
        }
    }

    private static void checkConsistency(File ids, File refs, File missing, String trackRoot) throws IOException {
        System.out.println("Starting consistency check");
        Stopwatch watch = createStarted();

        FileLineDifferenceIterator iter = new FileLineDifferenceIterator(ids, refs, new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                if (input != null) {
                    return input.split(DELIM)[0];
                }
                return "";
            }});


        // write the candidates identified to a temp file
        File candTemp = createTempFile("candTemp", null);
        int candidates = writeStrings(iter, candTemp, true);

        try {
            // retrieve the .del file from track directory
            File trackingFileParent = new File(FilenameUtils.concat(trackRoot, "blobids"));
            if (trackingFileParent.exists()) {
                Collection<File> files =
                    listFiles(trackingFileParent, FileFilterUtils.suffixFileFilter(".del"), null);

                // If a delete file is present filter the tracked deleted ids
                if (!files.isEmpty()) {
                    File delFile = files.iterator().next();
                    FileLineDifferenceIterator filteringIter = new FileLineDifferenceIterator(delFile, candTemp, new Function<String, String>() {
                        @Nullable @Override public String apply(@Nullable String input) {
                            if (input != null) {
                                return input.split(DELIM)[0];
                            }
                            return "";
                        }
                    });
                    candidates = FileIOUtils.writeStrings(filteringIter, missing, true);
                }
            } else {
                System.out.println("Skipping active deleted tracked as parameter [repoHome] : [" + trackRoot + "] incorrect");
                FileUtils.copyFile(candTemp, missing);
            }
        } finally {
            FileUtils.forceDelete(candTemp);
        }

        System.out.println("Consistency check found " + candidates + " missing blobs");
        if (candidates > 0) {
            System.out.println("Consistency check failure for the data store");
        }
        System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    private static void retrieveBlobReferences(GarbageCollectableBlobStore blobStore,
            BlobReferenceRetriever marker, File marked) throws IOException {
        final BufferedWriter writer = Files.newWriter(marked, Charsets.UTF_8);
        final AtomicInteger count = new AtomicInteger();
        boolean threw = true;
        try {
            final Joiner delimJoiner = Joiner.on(DELIM).skipNulls();
            final GarbageCollectableBlobStore finalBlobStore = blobStore;

            System.out.println("Starting dump of blob references");
            Stopwatch watch = createStarted();

            marker.collectReferences(
                new ReferenceCollector() {
                    @Override
                    public void addReference(String blobId, String nodeId) {
                        try {
                            Iterator<String> idIter = finalBlobStore.resolveChunks(blobId);

                            while (idIter.hasNext()) {
                                String id = delimJoiner.join(idIter.next(), nodeId);
                                count.getAndIncrement();
                                writeAsLine(writer, id, true);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error in retrieving references", e);
                        }
                    }
                }
            );
            writer.flush();
            sort(marked, new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
                }
            });
            System.out.println(count.get() + " blob references found");
            System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
            threw = false;
        } finally {
            close(writer, threw);
        }
    }

    private static void retrieveBlobIds(GarbageCollectableBlobStore blobStore, File blob)
        throws Exception {

        System.out.println("Starting dump of blob ids");
        Stopwatch watch = createStarted();

        Iterator<String> blobIter = blobStore.getAllChunkIds(0);
        int count = writeStrings(blobIter, blob, true);

        sort(blob);
        System.out.println(count + " blob ids found");
        System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
    }
}
