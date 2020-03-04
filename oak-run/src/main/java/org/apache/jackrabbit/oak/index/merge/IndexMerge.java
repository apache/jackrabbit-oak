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
package org.apache.jackrabbit.oak.index.merge;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jcr.PropertyType;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.json.TypeCodes;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Merge custom index definitions with out-of-the-box index definitions.
 */
public class IndexMerge {

    private final static Logger LOG = LoggerFactory.getLogger(IndexMerge.class);

    public static final String OAK_CHILD_ORDER = ":childOrder";

    private EditorHook hook;
    private ExecutorService executorService;

    private boolean quiet;

    public static void main(String... args) throws Exception {
        new IndexMerge().execute(args);
    }

    /**
     * Execute the command.
     *
     * @param args the command line arguments
     */
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Void> quietOption = parser.accepts("quiet", "be less chatty");
        OptionSpec<String> indexDirectory = parser.accepts("indexDir", "Index directory").
                withRequiredArg();
        Options opts = new Options();
        OptionSet options = opts.parseAndConfigure(parser, args);
        quiet = options.has(quietOption);
        boolean isReadWrite = opts.getCommonOpts().isReadWrite();
        boolean success = true;
        String indexRootDir = indexDirectory.value(options);
        if (indexRootDir == null) {
            throw new IllegalArgumentException("Required argument indexDir missing");
        }
        if (!isReadWrite) {
            log("Repository connected in read-only mode. Use '--read-write' for write operations");
        }
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            NodeStore nodeStore = fixture.getStore();
            BlobStore blobStore = fixture.getBlobStore();
            if (isReadWrite) {
                if (blobStore == null) {
                    throw new IllegalArgumentException("No blob store specified");
                }
                if (!(blobStore instanceof GarbageCollectableBlobStore)) {
                    throw new IllegalArgumentException("Not a garbage collectable blob store: " + blobStore);
                }
            }
            initHook(indexRootDir, (GarbageCollectableBlobStore) blobStore);

            JsonObject indexes = getIndexDefinitions(nodeStore);
            // the superseded indexes of the old repository
            List<String> supersededKeys = new ArrayList<>(getSupersededIndexDefs(indexes));
            Collections.sort(supersededKeys);

            // keep only new indexes that are not superseded
            Map<String, JsonObject> indexMap = indexes.getChildren();
            for (String superseded : supersededKeys) {
                if (indexMap.containsKey(superseded)) {
                    log("Ignoring superseded index " + superseded);
                    indexMap.remove(superseded);
                }
            }
            Set<String> indexKeys = indexes.getChildren().keySet();

            IndexDefMergerUtils.merge(indexes, indexes);

            Set<String> newIndexKeys =  new HashSet<>(indexes.getChildren().keySet());
            newIndexKeys.removeAll(indexKeys);
            if (newIndexKeys.isEmpty()) {
                log("No indexes to merge");
            }
            for (String newIndexKey : newIndexKeys) {
                log("New index: " + newIndexKey);
                JsonObject merged = indexMap.get(newIndexKey);
                String def = merged.toString();
                log("Merged definition: " + def);
                if (isReadWrite) {
                    storeIndex(nodeStore, newIndexKey, merged);
                }
            }
        }
        if (executorService != null) {
            executorService.shutdown();
        }
        if (!success) {
            System.exit(1);
        }
    }

    private void storeIndex(NodeStore ns, String newIndexName, JsonObject indexDef) {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder b = rootBuilder;
        for(String p : PathUtils.elements(newIndexName)) {
            b = b.child(p);
        }
        build("  ", b, indexDef);
        try {
            ns.merge(rootBuilder, hook, CommitInfo.EMPTY);
            log("Added index " + newIndexName);
        } catch (CommitFailedException e) {
            LOG.error("Failed to add index " + newIndexName, e);
        }
    }

    private void build(String linePrefix, NodeBuilder builder, JsonObject json) {
        for(Entry<String, String> e : json.getProperties().entrySet()) {
            String k = e.getKey();
            String value = e.getValue();
            JsopTokenizer tokenizer = new JsopTokenizer(value);
            if (tokenizer.matches('[')) {
                ArrayList<String> list = new ArrayList<>();
                while (!tokenizer.matches(']')) {
                   String jsonString = tokenizer.getToken();
                   list.add(jsonString);
                   tokenizer.matches(',');
                }
                log(linePrefix + "array " + k + " = " + list + " (String[])");
                builder.setProperty(k, list, Type.STRINGS);
            } else if (tokenizer.matches(JsopReader.TRUE)) {
                log(linePrefix + "property " + k + " = true (Boolean)");
                builder.setProperty(k, true);
            } else if (tokenizer.matches(JsopReader.FALSE)) {
                log(linePrefix + "property " + k + " = false (Boolean)");
                builder.setProperty(k, false);
            } else if (tokenizer.matches(JsopReader.STRING)) {
                String jsonString = tokenizer.getToken();
                int split = TypeCodes.split(jsonString);
                if (split != -1) {
                    int type = TypeCodes.decodeType(split, jsonString);
                    String v = TypeCodes.decodeName(split, jsonString);
                    if (type == PropertyType.BINARY) {
                        throw new UnsupportedOperationException();
                    } else {
                        builder.setProperty(PropertyStates.createProperty(k, v, type));
                    }
                }
            } else if (tokenizer.matches(JsopReader.NUMBER)) {
                String num = tokenizer.getToken();
                boolean isDouble = num.indexOf('.') >= 0;
                if (isDouble) {
                    double d = Double.parseDouble(num);
                    log(linePrefix + "property " + k + " = " + d + " (Double)");
                    builder.setProperty(k, d);
                } else {
                    long x = Long.parseLong(num);
                    log(linePrefix + "property " + k + " = " + x + " (Long)");
                    builder.setProperty(k, x);
                }
            }
        }
        ArrayList<String> childOrder = new ArrayList<>();
        for(Entry<String, JsonObject> e : json.getChildren().entrySet()) {
            String k = e.getKey();
            JsonObject el = e.getValue();
            log(linePrefix + "child " + k);
            build(linePrefix + "  ", builder.child(k), (JsonObject) el);
            childOrder.add(k);
        }
        if (!childOrder.isEmpty()) {
            builder.setProperty(OAK_CHILD_ORDER, childOrder, Type.NAMES);
        }
    }

    /**
     * Get the names of the index definitions that are superseded in one of the
     * indexes.
     *
     * @param indexDefs all index definitions
     * @return the superseded indexes
     */
    public static Set<String> getSupersededIndexDefs(JsonObject indexDefs) {
        HashSet<String> supersededIndexes = new HashSet<>();
        for(JsonObject d : indexDefs.getChildren().values()) {
            String supersedes = d.getProperties().get("supersedes");
            if (supersedes != null) {
                JsopTokenizer tokenizer = new JsopTokenizer(supersedes);
                if (tokenizer.matches('[')) {
                    while (!tokenizer.matches(']')) {
                        if (tokenizer.matches(JsopReader.STRING)) {
                            String s = tokenizer.getToken();
                            if (!s.contains("/@")) {
                                supersededIndexes.add(s);
                            }
                        } else {
                            throw new IllegalArgumentException("Unexpected token: " + tokenizer.getToken());
                        }
                        tokenizer.matches(',');
                    }
                }
            }
        }
        return supersededIndexes;
    }

    /**
     * Get the the index definitions from a node store. It uses the index path
     * service and index definition printer from Oak.
     *
     * @param nodeStore the source node store
     * @return a JSON object with all index definitions
     */
    private static JsonObject getIndexDefinitions(NodeStore nodeStore) throws IOException {
        IndexPathService imageIndexPathService = new IndexPathServiceImpl(nodeStore);
        IndexDefinitionPrinter indexDefinitionPrinter = new IndexDefinitionPrinter(nodeStore, imageIndexPathService);
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        indexDefinitionPrinter.print(printWriter, Format.JSON, false);
        printWriter.flush();
        writer.flush();
        String json = writer.toString();
        return JsonObject.fromJson(json, true);
    }

    private void log(String message) {
        if(!quiet) {
            System.out.println(message);
        }
    }

    private void initHook(String indexRootDir, GarbageCollectableBlobStore blobStore) throws IOException {
        IndexTracker tracker = new IndexTracker();
        executorService = Executors.newFixedThreadPool(2);
        IndexCopier indexCopier = new IndexCopier(executorService, new File(indexRootDir));
        MountInfoProvider mip = createMountInfoProvider();
        LuceneIndexEditorProvider luceneEditor = new LuceneIndexEditorProvider(indexCopier, tracker, null, null, mip);
        luceneEditor.setBlobStore(blobStore);
        CompositeIndexEditorProvider indexEditor = new CompositeIndexEditorProvider(
                luceneEditor,
                new PropertyIndexEditorProvider().with(mip),
                new ReferenceEditorProvider().with(mip),
                new NodeCounterEditorProvider().with(mip)
        );
        IndexUpdateProvider updateProvider = new IndexUpdateProvider(
                indexEditor, "async", false);
        hook = new EditorHook(updateProvider);
    }

    private static MountInfoProvider createMountInfoProvider() {
        // TODO probably need the ability to configure mounts
        return Mounts.newBuilder()
                .mount("libs", true, Arrays.asList(
                        // pathsSupportingFragments
                        "/oak:index/*$"
                ), Arrays.asList(
                        // mountedPaths
                        "/libs",
                        "/apps",
                        "/jcr:system/rep:permissionStore/oak:mount-libs-crx.default"))
                .build();
    }

}
