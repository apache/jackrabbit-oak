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
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelinedMergeSortTaskTest {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTaskTest.class);
    private final ClassLoader classLoader = getClass().getClassLoader();
    private final PathElementComparator pathComparator = new PathElementComparator(Set.of());
    private final Compression algorithm = Compression.NONE;
    @Rule
    public TemporaryFolder sortFolder = new TemporaryFolder();

    @Test
    public void noFileToMerge() throws Exception {
        PipelinedMergeSortTask.Result result = runTest();
        Path resultFile = result.getFlatFileStoreFile().toPath();
        assertEquals(0, Files.size(resultFile));
    }

    @Test
    public void oneFileToMerge() throws Exception {
        File singleFileToMerge = getTestFile("pipelined/merge-stage-1.json");
        PipelinedMergeSortTask.Result result = runTest(singleFileToMerge);
        Path resultFile = result.getFlatFileStoreFile().toPath();
        assertEquals(Files.readString(singleFileToMerge.toPath(), FLATFILESTORE_CHARSET), Files.readString(resultFile, FLATFILESTORE_CHARSET));
    }

    @Test
    public void twoFilesToMerge() throws Exception {
        File merge1 = getTestFile("pipelined/merge-stage-1.json");
        File merge2 = getTestFile("pipelined/merge-stage-2.json");
        File expected = getTestFile("pipelined/merge-expected.json");

        PipelinedMergeSortTask.Result result = runTest(merge1, merge2);
        Path resultFile = result.getFlatFileStoreFile().toPath();
        LOG.info("Result: {}\n{}", resultFile, Files.readString(resultFile, FLATFILESTORE_CHARSET));
        assertEquals(Files.readString(expected.toPath(), FLATFILESTORE_CHARSET), Files.readString(resultFile, FLATFILESTORE_CHARSET));
    }

    private File getTestFile(String name) {
        URL url = classLoader.getResource(name);
        if (url == null) throw new IllegalArgumentException("Test file not found: " + name);
        return new File(url.getPath());
    }

    private PipelinedMergeSortTask.Result runTest(File... files) throws Exception {
        File sortRoot = sortFolder.getRoot();
        // +1 for the Sentinel.
        ArrayBlockingQueue<File> sortedFilesQueue = new ArrayBlockingQueue<>(files.length + 1);
        PipelinedMergeSortTask mergeSortTask = new PipelinedMergeSortTask(sortRoot, pathComparator, algorithm, sortedFilesQueue);
        // Enqueue all the files that are to be merged
        for (File file : files) {
            // The intermediate files are deleted after being merged, so we should copy them to the temporary sort root folder
            Path workDirCopy = Files.copy(file.toPath(), sortRoot.toPath().resolve(file.getName()));
            sortedFilesQueue.put(workDirCopy.toFile());
        }
        // Signal end of files to merge
        sortedFilesQueue.put(SENTINEL_SORTED_FILES_QUEUE);
        // Run the merge task
        PipelinedMergeSortTask.Result result = mergeSortTask.call();
        File[] filesInWorkDir = sortRoot.listFiles();
        if (filesInWorkDir == null) throw new IllegalStateException("The sort work directory is not a directory: " + sortRoot);
        assertEquals("The sort work directory should contain only the flat file store, the intermediate files should have been deleted after merged. Instead it contains: " + Arrays.toString(filesInWorkDir),
                1, filesInWorkDir.length);
        assertTrue(result.getFlatFileStoreFile().exists());
        return result;
    }

    @Test(expected = IllegalStateException.class)
    public void badInputFile() throws Exception {
        File singleFileToMerge = createFileWithWrongFormat();
        runTest(singleFileToMerge);
    }

    private File createFileWithWrongFormat() throws Exception {
        File file = Files.createTempFile("merge-stage-input", ".json").toFile();
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(), FLATFILESTORE_CHARSET)) {
            bw.write("/a/b/c\n");
        }
        file.deleteOnExit();
        return file;
    }
}
