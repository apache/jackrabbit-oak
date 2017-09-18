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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;

public class CompositePrepareCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<?> help = parser
                .acceptsAll(asList("h", "?", "help"), "show help")
                .forHelp();
        OptionSpec<String> paths = parser.accepts("paths", "a list of paths to transform from nt:resource to oak:Resource")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesSeparatedBy(',')
                .defaultsTo("/apps", "/libs");
        OptionSpec<File> storeO = parser.nonOptions("path to segment store (required)")
                .ofType(File.class);
        OptionSet options = parser.parse(args);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        File store = storeO.value(options);

        if (store == null) {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(store);
        FileStore fs = builder.build();
        try {
            new OakResourceTransformer(fs, paths.values(options)).transform();
        } finally {
            fs.close();
        }
    }

    public static class OakResourceTransformer {

        private static final Logger LOG = LoggerFactory.getLogger(CompositePrepareCommand.class);

        private final FileStore fileStore;

        private final List<String> paths;

        private int totalNodes;

        private int modifiedNodes;

        public OakResourceTransformer(FileStore fileStore, List<String> paths) {
            this.paths = paths;
            this.fileStore = fileStore;
        }

        public void transform() throws InterruptedException, IOException {
            SegmentNodeState headNodeState = fileStore.getHead();
            SegmentNodeBuilder superRootBuilder = headNodeState.builder();
            for (String cp : superRootBuilder.getChildNode("checkpoints").getChildNodeNames()) {
                LOG.info("Transforming checkpoint {}", cp);
                NodeBuilder cpRoot = superRootBuilder.getChildNode("checkpoints").getChildNode(cp).getChildNode("root");
                transformRootBuilder(cpRoot);
            }
            LOG.info("Transforming root");
            transformRootBuilder(superRootBuilder.getChildNode("root"));
            fileStore.getRevisions().setHead(headNodeState.getRecordId(), superRootBuilder.getNodeState().getRecordId());
            fileStore.flush();
        }

        private void transformRootBuilder(NodeBuilder rootBuilder) {
            for (String p : paths) {
                NodeBuilder builder = rootBuilder;
                for (String e : PathUtils.elements(p)) {
                    builder = builder.getChildNode(e);
                }
                if (builder.exists()) {
                    totalNodes = modifiedNodes = 0;
                    LOG.info("  path: {}", p);
                    NodeBuilder indexData = rootBuilder
                            .child(IndexConstants.INDEX_DEFINITIONS_NAME)
                            .child("uuid")
                            .child(IndexConstants.INDEX_CONTENT_NODE_NAME);
                    transformBuilder(builder, indexData);
                    LOG.info("    all nodes: {}, updated nodes: {}", totalNodes, modifiedNodes);
                }
            }
        }

        private void transformBuilder(NodeBuilder builder, NodeBuilder uuidIndexData) {
            String type = builder.getName(NodeTypeConstants.JCR_PRIMARYTYPE);
            if (NodeTypeConstants.NT_RESOURCE.equals(type)) {
                String index = builder.getString(JCR_UUID);
                uuidIndexData.getChildNode(index).remove();

                builder.setProperty(NodeTypeConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_RESOURCE, Type.NAME);
                builder.removeProperty(JCR_UUID);

                modifiedNodes++;
            }
            for (String child : builder.getChildNodeNames()) {
                transformBuilder(builder.getChildNode(child), uuidIndexData);
            }
            totalNodes++;
        }
    }

}
