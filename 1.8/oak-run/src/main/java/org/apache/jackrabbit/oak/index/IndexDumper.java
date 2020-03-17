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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.exporter.NodeStateSerializer;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LuceneIndexDumper;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

public class IndexDumper {
    public static final String INDEX_DUMPS_DIR = "index-dumps";
    private final IndexHelper indexHelper;
    private final File outDir;

    public IndexDumper(IndexHelper indexHelper, File outDir) {
        this.indexHelper = indexHelper;
        this.outDir = outDir;
    }

    public void dump() throws IOException {
        NodeState root = indexHelper.getNodeStore().getRoot();
        File indexDumpDir = new File(outDir, INDEX_DUMPS_DIR);

        FileUtils.forceMkdir(indexDumpDir);
        long totalSize = 0;
        int indexCount = 0;

        for (String indexPath : indexHelper.getIndexPathService().getIndexPaths()) {
            NodeState indexState = NodeStateUtils.getNode(root, indexPath);
            if (!TYPE_LUCENE.equals(indexState.getString(TYPE_PROPERTY_NAME))) {
                continue;
            }
            LuceneIndexDumper dumper = new LuceneIndexDumper(root, indexPath, indexDumpDir);
            try {
                dumper.dump();
                System.out.printf("    - %s (%s)%n", indexPath, IOUtils.humanReadableByteCount(dumper.getSize()));
            } catch (Exception e){
                System.out.printf("Error occurred while performing consistency check for index [%s]%n", indexPath);
                e.printStackTrace(System.out);
                try {
                    File indexDir = DirectoryUtils.createIndexDir(indexDumpDir, indexPath);
                    NodeStateSerializer serializer = new NodeStateSerializer(root);
                    serializer.setPath(indexPath);
                    serializer.setSerializeBlobContent(true);
                    serializer.serialize(indexDir);
                    System.out.printf("    - Dumping raw node content%n", indexPath, FileUtils.sizeOf(indexDir));
                    System.out.printf("    - %s (%s)%n", indexPath, FileUtils.sizeOf(indexDir));
                } catch (Exception e2){
                    System.out.printf("Error occurred while performing consistency check for index [%s]%n", indexPath);
                    e2.printStackTrace(System.out);
                }
            }

            indexCount++;
            totalSize += dumper.getSize();
        }

        System.out.printf("Dumped index data of %d indexes (%s) to %s%n", indexCount,
                IOUtils.humanReadableByteCount(totalSize), IndexCommand.getPath(indexDumpDir));
    }
}
