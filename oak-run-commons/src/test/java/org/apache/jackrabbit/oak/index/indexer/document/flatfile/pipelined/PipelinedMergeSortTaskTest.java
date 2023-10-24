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
import org.junit.Test;

import java.io.BufferedWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelinedMergeSortTaskTest extends PipelinedMergeSortTaskTestBase {
    private final ClassLoader classLoader = getClass().getClassLoader();
    private final Compression algorithm = Compression.NONE;

    @Test
    public void noFileToMerge() throws Exception {
        PipelinedMergeSortTask.Result result = runTest(algorithm);
        Path resultFile = result.getFlatFileStoreFile();
        assertEquals(0, Files.size(resultFile));
    }

    @Test
    public void oneFileToMerge() throws Exception {
        Path singleFileToMerge = getTestFile("pipelined/merge-stage-1.json");
        PipelinedMergeSortTask.Result result = runTest(algorithm, singleFileToMerge);
        Path resultFile = result.getFlatFileStoreFile();
        assertEquals(Files.readAllLines(singleFileToMerge, FLATFILESTORE_CHARSET), Files.readAllLines(resultFile, FLATFILESTORE_CHARSET));
    }

    @Test
    public void twoFilesToMerge() throws Exception {
        Path merge1 = getTestFile("pipelined/merge-stage-1.json");
        Path merge2 = getTestFile("pipelined/merge-stage-2.json");
        Path expected = getTestFile("pipelined/merge-expected.json");

        PipelinedMergeSortTask.Result result = runTest(algorithm, merge1, merge2);
        Path resultFile = result.getFlatFileStoreFile();
        log.info("Result: {}\n{}", resultFile, Files.readString(resultFile, FLATFILESTORE_CHARSET));
        assertEquals(Files.readAllLines(expected, FLATFILESTORE_CHARSET), Files.readAllLines(resultFile, FLATFILESTORE_CHARSET));
    }

    private Path getTestFile(String name) {
        URL url = classLoader.getResource(name);
        if (url == null) throw new IllegalArgumentException("Test file not found: " + name);
        return Paths.get(url.getPath());
    }

    private PipelinedMergeSortTask.Result runTest(Compression algorithm, Path... files) throws Exception {
        Path sortRoot = sortFolder.getRoot().toPath();
        // +1 for the Sentinel.
        ArrayBlockingQueue<Path> sortedFilesQueue = new ArrayBlockingQueue<>(files.length + 1);
        PipelinedMergeSortTask mergeSortTask = new PipelinedMergeSortTask(sortRoot, pathComparator, algorithm, sortedFilesQueue);
        // Enqueue all the files that are to be merged
        for (Path file : files) {
            // The intermediate files are deleted after being merged, so we should copy them to the temporary sort root folder
            Path workDirCopy = Files.copy(file, sortRoot.resolve(file.getFileName()));
            sortedFilesQueue.put(workDirCopy);
        }
        // Signal end of files to merge
        sortedFilesQueue.put(SENTINEL_SORTED_FILES_QUEUE);
        // Run the merge task
        PipelinedMergeSortTask.Result result = mergeSortTask.call();

        try (Stream<Path> fileStream = Files.list(sortRoot)) {
            List<String> filesInWorkDir = fileStream
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
            assertEquals("The sort work directory should contain only the flat file store, the intermediate files should have been deleted after merged. Instead it contains: " + filesInWorkDir,
                    1, filesInWorkDir.size());
        }
        assertTrue(Files.exists(result.getFlatFileStoreFile()));
        return result;
    }

    @Test(expected = IllegalStateException.class)
    public void badInputFile() throws Exception {
        Path singleFileToMerge = createFileWithWrongFormat();
        runTest(algorithm, singleFileToMerge);
    }

    private Path createFileWithWrongFormat() throws Exception {
        Path file = Files.createTempFile(sortFolder.getRoot().toPath(), "merge-stage-input", ".json");
        try (BufferedWriter bw = Files.newBufferedWriter(file, FLATFILESTORE_CHARSET)) {
            bw.write("/a/b/c\n");
        }
        return file;
    }

    @Test
    public void manyFilesToMergeDidNotMerge() throws Exception {
        int intermediateFilesCount = 256;
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, "20");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, "1000");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, "1");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, "1000");

        // Generate FFS
        List<String> ffs = generateFFS(LINES_IN_FFS);
        // Shuffle entries to simulate retrieving from MongoDB by an arbitrary order
        Collections.shuffle(ffs);

        // Generate the expected results by sorting using the node state entries comparator,
        List<NodeStateHolder> nodesOrdered = sortAsNodeStateEntries(ffs);
        // Convert back to a list of Strings
        String[] expectedFFS = nodesOrdered.stream().map(f -> new String(f.getLine())).toArray(String[]::new);

        // Write intermediate files
        List<Path> intermediateFiles = createIntermediateFiles(ffs, intermediateFilesCount);

        // Run test
        PipelinedMergeSortTask.Result result = runTestLargeFiles(Compression.NONE, intermediateFiles.toArray(new Path[0]));
        Path resultFile = result.getFlatFileStoreFile();

        assertEquals(intermediateFilesCount, result.getIntermediateFilesCount());
        assertEquals(0, result.getEagerMergeRuns());
        assertEquals(intermediateFilesCount, result.getFinalMergeFilesCount());

        // Verify result
        List<String> actualFFS = Files.readAllLines(resultFile);
        assertArrayEquals(expectedFFS, actualFFS.toArray(new String[0]));
    }
}
