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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import static org.apache.jackrabbit.guava.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This util class can be used to export a tree (eg entire repository) to a flat
 * file, without index dependency/involvement.
 */
public class SimpleFlatFileUtil {

    private static final Logger log = LoggerFactory.getLogger(SimpleFlatFileUtil.class);

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private final BufferedWriter bw;
    private final NodeStateEntryWriter entryWriter;
    private long totalLines = 0;

    private SimpleFlatFileUtil(BufferedWriter bw) throws IOException {
        // blobStore is only used for deserialization - so pass null here:
        entryWriter = new NodeStateEntryWriter(null);
        this.bw = bw;
    }

    public static void createFlatFileFor(NodeState ns, File f) throws IOException {
        log.info("createFlatFileFor : writing to {}", f.getCanonicalPath());
        try (BufferedWriter bw = Files.newBufferedReader(f.toPath())) {
            SimpleFlatFileUtil h = new SimpleFlatFileUtil(bw);
            h.addEntryAndTraverseChildren(ns);
            log.info("createFlatFileFor : done. wrote {} lines in total.", h.totalLines);
        }
    }

    private void addEntryAndTraverseChildren(NodeState ns) throws IOException {
        addEntry(ns);
        for (ChildNodeEntry e : ns.getChildNodeEntries()) {
            addEntryAndTraverseChildren(e.getNodeState());
        }
    }

    private void addEntry(NodeState ns) throws IOException {
        DocumentNodeState dns = (DocumentNodeState) ns;
        NodeStateEntry e = new NodeStateEntry.NodeStateEntryBuilder(dns,
                dns.getPath().toString()).build();
        String path = e.getPath();
        if (NodeStateUtils.isHiddenPath(path)) {
            // skip
            return;
        }
        String jsonText = entryWriter.asJson(e.getNodeState());
        String line = entryWriter.toString(copyOf(elements(path)), jsonText);
        bw.append(line);
        bw.append(LINE_SEPARATOR);
        totalLines++;
        if (totalLines % 10000 == 0) {
            log.info("addEntry : wrote {} lines so far.", totalLines);
        }
    }
}