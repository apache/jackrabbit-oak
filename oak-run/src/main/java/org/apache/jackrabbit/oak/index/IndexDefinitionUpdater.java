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
package org.apache.jackrabbit.oak.index;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.*;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

public class IndexDefinitionUpdater {

    private final NodeStore store;

    private IndexDefinitionUpdater() {
        store = new MemoryNodeStore();
        // initialize as a JCR repo to register namespaces and create initial index structure (/oak:index for our use)
        new Jcr(store).createRepository();
    }

    public static void main(String [] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("in", "input index definition file").withRequiredArg().required();
        parser.accepts("out", "output index definition file").withRequiredArg();
        parser.accepts("initializer", "implementation of RepositoryInitializer to update index definitions").withRequiredArg().required();

        OptionSet optionSet = parser.parse(args);

        Validate.checkArgument(optionSet.has("in"), "input index definition must be provided");
        String inFilePath = optionSet.valueOf("in").toString();

        Validate.checkArgument(optionSet.has("initializer"), "initializer class must be provided");
        String initializerClassName = optionSet.valueOf("initializer").toString();
        Class<?> repoInitClazz = Class.forName(initializerClassName);
        Object obj = repoInitClazz.newInstance();
        Validate.checkArgument(obj instanceof RepositoryInitializer, repoInitClazz + " is not a RepositoryInitializer");

        String outFilePath = optionSet.has("out") ? optionSet.valueOf("out").toString() : null;

        new IndexDefinitionUpdater().process(inFilePath, outFilePath, (RepositoryInitializer)obj);
    }

    private void process(String inFilePath, String outFilePath, RepositoryInitializer initializer) throws Exception {
        NodeState before = setupBaseState(inFilePath);
        NodeState after = applyInitializer(before.builder(), initializer);

        List<String> reindexingDefPaths = getReindexIndexPaths();
        List<String> reindexingLuceneDefPaths = getLuceneIndexPaths(reindexingDefPaths);

        if (outFilePath != null) {
            writeReindexingLuceneDefs(outFilePath, reindexingLuceneDefPaths);
        }
        dumpIndexDiff(before, after, reindexingDefPaths);
    }

    private NodeState setupBaseState(String inFilePath) throws Exception {
        org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater indexDefinitionUpdater = new org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater(new File(inFilePath));

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder oiBuilder = builder.getChildNode(INDEX_DEFINITIONS_NAME);

        for (String indexPath : indexDefinitionUpdater.getIndexPaths()) {
            oiBuilder.setChildNode(PathUtils.getName(indexPath), indexDefinitionUpdater.getIndexState(indexPath));
        }

        return merge(builder);
    }

    private NodeState applyInitializer(NodeBuilder builder, RepositoryInitializer initializer) throws CommitFailedException {
        initializer.initialize(builder);
        return merge(builder);
    }

    private void writeReindexingLuceneDefs(String outFilePath, List<String> reindexingLuceneDefPaths) throws IOException {
        IndexPathService indexPathService = () -> reindexingLuceneDefPaths;

        IndexDefinitionPrinter printer = new IndexDefinitionPrinter(store, indexPathService);

        try (PrintWriter pw = new PrintWriter(
                new BufferedOutputStream(
                        FileUtils.openOutputStream(new File(outFilePath))))) {
            printer.print(pw, Format.JSON, false);
        }
    }

    private List<String> getReindexIndexPaths() {
        return StreamSupport.stream(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNodeEntries().spliterator(), false)
                .filter(cne -> !cne.getNodeState().hasProperty(REINDEX_PROPERTY_NAME) || cne.getNodeState().getBoolean(REINDEX_PROPERTY_NAME))
                .filter(cne -> cne.getNodeState().hasProperty(TYPE_PROPERTY_NAME))
                .sorted(comparing(cne -> cne.getNodeState().getString(TYPE_PROPERTY_NAME)))
                .map(cne -> "/" + INDEX_DEFINITIONS_NAME + "/" + cne.getName()).collect(Collectors.toList())
                ;
    }

    private List<String> getLuceneIndexPaths(List<String> indexDefPaths) {
        NodeState root = store.getRoot();
        return indexDefPaths.stream()
                .filter(path -> "lucene".equals(NodeStateUtils.getNode(root, path).getString(TYPE_PROPERTY_NAME)))
                .collect(Collectors.toList());
    }

    private NodeState merge(NodeBuilder builder) throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return store.getRoot();
    }

    private void dumpIndexDiff(NodeState beforeRoot, NodeState afterRoot, List<String> reindexingLuceneDefPaths) {
        try (PrintWriter pw = new PrintWriter(System.out)) {
            for (String indexPath : reindexingLuceneDefPaths) {
                NodeState before = NodeStateUtils.getNode(beforeRoot, indexPath);
                NodeState after = NodeStateUtils.getNode(afterRoot, indexPath);

                pw.println("");
                pw.println("------ " + indexPath + " --------");
                after.compareAgainstBaseState(before, new VisibleChangesDiff(pw));
            }
        }
    }

    static class VisibleChangesDiff implements NodeStateDiff {
        private static final String INDENT = "  ";

        private static final Function<Blob, String> BLOB_LENGTH = new Function<Blob, String>() {

            @Override
            public String apply(Blob b) {
                return safeGetLength(b);
            }

            private String safeGetLength(Blob b) {
                try {
                    return byteCountToDisplaySize(b.length());
                } catch (IllegalStateException e) {
                    // missing BlobStore probably
                }
                return "[N/A]";
            }

        };

        private final PrintWriter pw;
        private final String indent;
        private boolean isRoot;

        VisibleChangesDiff(PrintWriter pw) {
            this(pw, INDENT);
            isRoot = true;
        }

        private VisibleChangesDiff(PrintWriter pw, String indent) {
            this.pw = pw;
            this.indent = indent;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (!after.getName().startsWith(":")) {
                pw.println(indent + "+ " + toString(after));
            }
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (before.getName().equals("reindex") && isRoot) {
                // don't care if reindex changed on top level
                return true;
            }

            if (before.getName().startsWith(":")) {
                return propertyAdded(after);
            } else if (after.getName().startsWith(":")) {
                return propertyDeleted(before);
            } else {
                pw.println(indent + "^ " + before.getName());
                pw.println(indent + "  - " + toString(before));
                pw.println(indent + "  + " + toString(after));
            }
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (!before.getName().startsWith(":")) {
                pw.println(indent + "- " + toString(before));
            }
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (name.startsWith(":")) {
                return true;
            }
            pw.println(indent + "+ " + name);
            return after.compareAgainstBaseState(EMPTY_NODE, new VisibleChangesDiff(
                    pw, indent + INDENT));
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (name.startsWith(":")) {
                return true;
            }

            StringWriter sw = new StringWriter();
            boolean ret;
            try (PrintWriter subPw = new PrintWriter(sw)) {
                ret = after.compareAgainstBaseState(before,
                        new VisibleChangesDiff(subPw, indent + INDENT));
            }

            if (sw.getBuffer().length() > 0) {
                pw.println(indent + "^ " + name);
                pw.print(sw.toString());
            }

            return ret;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (name.startsWith(":")) {
                return true;
            }
            pw.println(indent + "- " + name);
            return MISSING_NODE.compareAgainstBaseState(before, new VisibleChangesDiff(pw, indent + INDENT));
        }

        private static String toString(PropertyState ps) {
            StringBuilder val = new StringBuilder();
            val.append(ps.getName()).append("<").append(ps.getType()).append(">");
            if (ps.getType() == BINARY) {
                String v = BLOB_LENGTH.apply(ps.getValue(BINARY));
                val.append(" = {").append(v).append("}");
            } else if (ps.getType() == BINARIES) {
                String v = StreamSupport.stream(ps.getValue(BINARIES).spliterator(), false)
                        .map(BLOB_LENGTH)
                        .collect(Collectors.toList()).toString();
                val.append("[").append(ps.count()).append("] = ").append(v);
            } else if (ps.isArray()) {
                val.append("[").append(ps.count()).append("] = ").append(ps.getValue(STRINGS));
            } else {
                val.append(" = ").append(ps.getValue(STRING));
            }
            return ps.getName() + "<" + ps.getType() + ">" + val.toString();
        }
    }
}
