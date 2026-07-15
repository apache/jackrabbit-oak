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

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.commons.collections.ListUtils;
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

    private final Writer writer;
    private final NodeStateEntryWriter entryWriter;
    private long totalLines = 0;

    private SimpleFlatFileUtil(Writer writer) {
        // blobStore is only used for deserialization - so pass null here:
        this.entryWriter = new NodeStateEntryWriter(null, true);
        this.writer = writer;
    }

    public static void createFlatFileFor(NodeState ns, File f) throws IOException {
        log.info("createFlatFileFor : writing to {}", f.getCanonicalPath());
        try (BufferedWriter bw = Files.newBufferedWriter(f.toPath())) {
            createFlatFileFor(ns, bw);
        }
    }

    public static void createFlatFileFor(NodeState ns, Writer writer) throws IOException {
        SimpleFlatFileUtil h = new SimpleFlatFileUtil(writer);
        h.addEntryAndTraverseChildren(ns);
        log.info("createFlatFileFor : done. wrote {} lines in total.", h.totalLines);
    }

    private void addEntryAndTraverseChildren(NodeState ns) throws IOException {
        addEntry(ns);
        StreamSupport.stream(ns.getChildNodeEntries().spliterator(), false)
                .sorted(Comparator.comparing(ChildNodeEntry::getName)).forEach(e -> {
                    try {
                        addEntryAndTraverseChildren(e.getNodeState());
                    } catch (IOException e1) {
                        // NOSONAR
                        throw new RuntimeException(e1);
                    }
                });
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
        String jsonText = entryWriter.asSortedJson(e.getNodeState());
        String line = entryWriter.toString(ListUtils.toList(elements(path)), jsonText);
        writer.append(line);
        writer.append(LINE_SEPARATOR);
        totalLines++;
        if (totalLines % 10000 == 0) {
            log.info("addEntry : wrote {} lines so far.", totalLines);
        }
    }
}
