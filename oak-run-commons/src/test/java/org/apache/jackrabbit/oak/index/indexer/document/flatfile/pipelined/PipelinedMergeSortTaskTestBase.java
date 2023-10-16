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

import org.apache.jackrabbit.oak.commons.Compression;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelinedMergeSortTaskTestBase {
    static final int LINES_IN_FFS = 100000;
    static final PathElementComparator pathComparator = new PathElementComparator(Set.of());

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    @Rule
    public TemporaryFolder sortFolder = new TemporaryFolder();


    protected List<NodeStateHolder> sortAsNodeStateEntries(List<String> ffsLines) {
        Comparator<NodeStateHolder> comparatorBinary = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        NodeStateHolderFactory nodeFactory = new NodeStateHolderFactory();
        List<NodeStateHolder> nodesOrdered = ffsLines.stream()
                .map(ffsLine -> nodeFactory.apply(ffsLine.getBytes(FLATFILESTORE_CHARSET)))
                .sorted(comparatorBinary)
                .collect(Collectors.toList());
        return nodesOrdered;
    }


    protected List<String> generateFFS(int numberOfLines) {
        List<String> ffsLines = new ArrayList<>(numberOfLines);
        for (int i = 0; i < numberOfLines; i++) {
            String path = "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z/" + i;
            String entry = "{\"_id\":\"" + path + "\",\"property\":[{\"name\":\"jcr:primaryType\",\"values\":[\"nt:unstructured\"]}]}";
            ffsLines.add(path + "|" + entry);
        }
        return ffsLines;
    }

    protected List<Path> createIntermediateFiles(List<String> ffsLines, int numberOfFiles) throws Exception {
        Iterator<String> ffsIter = ffsLines.iterator();
        Path workFolder = sortFolder.newFolder("merge_many_test").toPath();
        ArrayList<Path> intermediateFiles = new ArrayList<>(numberOfFiles);
        int linesPerFile = ffsLines.size() / numberOfFiles;

        for (int fileIdx = 0; fileIdx < numberOfFiles; fileIdx++) {
            Path intermediateFile = workFolder.resolve("intermediate-" + fileIdx + ".json");
            ArrayList<String> linesInIntermediateFile = new ArrayList<>();
            while (linesInIntermediateFile.size() < linesPerFile && ffsIter.hasNext()) {
                linesInIntermediateFile.add(ffsIter.next());
            }
            if (fileIdx == numberOfFiles - 1) {
                // Add the remaining elements to the last file
                while (ffsIter.hasNext()) {
                    linesInIntermediateFile.add(ffsIter.next());
                }
            }
            List<NodeStateHolder> nodesSorted = sortAsNodeStateEntries(linesInIntermediateFile);
            try (BufferedWriter bw = Files.newBufferedWriter(intermediateFile, FLATFILESTORE_CHARSET)) {
                for (NodeStateHolder node : nodesSorted) {
                    bw.write(new String(node.getLine()));
                    bw.write("\n");
                }
            }
            intermediateFiles.add(intermediateFile);
        }
        return intermediateFiles;
    }

    protected PipelinedMergeSortTask.Result runTestLargeFiles(Compression algorithm, Path... files) throws Exception {
        Path sortRoot = sortFolder.getRoot().toPath();
        // +1 for the Sentinel.
        ArrayBlockingQueue<Path> sortedFilesQueue = new ArrayBlockingQueue<>(files.length + 1);
        PipelinedMergeSortTask mergeSortTask = new PipelinedMergeSortTask(sortRoot, pathComparator, algorithm, sortedFilesQueue);
        // Enqueue all the files that are to be merged
        for (Path file : files) {
            sortedFilesQueue.put(file);
        }
        // Signal end of files to merge
        sortedFilesQueue.put(SENTINEL_SORTED_FILES_QUEUE);
        // Run the merge task
        PipelinedMergeSortTask.Result result = mergeSortTask.call();
        List<Path> filesInWorkDir;
        try (Stream<Path> stream = Files.list(sortRoot)) {
            filesInWorkDir = stream.filter(Files::isRegularFile).collect(Collectors.toList());
        }
        assertEquals("The sort work directory should contain only the flat file store, the intermediate files should have been deleted after merged. Instead it contains: " + filesInWorkDir,
                1, filesInWorkDir.size());
        assertTrue(Files.exists(result.getFlatFileStoreFile()));
        return result;
    }
}
