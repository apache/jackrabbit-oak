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
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.LZ4Compression;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PipelinedMergeSortTaskParameterizedTest extends PipelinedMergeSortTaskTestBase {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // numberOfIntermediateFiles, eagerMergeTriggerThreshold
        return Arrays.asList(new Object[][]{
                {256, 50},
                {256, 300}
        });
    }

    private final int numberOfIntermediateFiles;
    private final int eagerMergeTriggerThreshold;

    public PipelinedMergeSortTaskParameterizedTest(int numberOfIntermediateFiles, int eagerMergeTriggerThreshold) {
        this.numberOfIntermediateFiles = numberOfIntermediateFiles;
        this.eagerMergeTriggerThreshold = eagerMergeTriggerThreshold;
    }

    @Test
    public void manyFilesToMerge() throws Exception {
        log.info("Running with intermediateFiles: {}, eagerMergeTriggerThreshold: {}", numberOfIntermediateFiles, eagerMergeTriggerThreshold);
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, Integer.toString(eagerMergeTriggerThreshold));
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, "32");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, "512");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, "8");

        // Generate FFS
        List<String> ffs = generateFFS(LINES_IN_FFS);
        // Shuffle entries to simulate retrieving from MongoDB by an arbitrary order
        Collections.shuffle(ffs);

        // Generate the expected results by sorting using the node state entries comparator,
        List<NodeStateHolder> nodesOrdered = sortAsNodeStateEntries(ffs);
        // Convert back to a list of Strings
        String[] expectedFFS = nodesOrdered.stream().map(f -> new String(f.getLine())).toArray(String[]::new);

        // Write intermediate files
        List<Path> intermediateFiles = createIntermediateFiles(ffs, this.numberOfIntermediateFiles);

        // Run test
        PipelinedMergeSortTask.Result result = runTestLargeFiles(Compression.NONE, intermediateFiles.toArray(new Path[0]));
        Path resultFile = result.getFlatFileStoreFile();

        assertEquals(this.numberOfIntermediateFiles, result.getIntermediateFilesCount());
        if (this.numberOfIntermediateFiles > eagerMergeTriggerThreshold) {
            assertTrue(result.getEagerMergeRuns() > 0);
            assertTrue(result.getFinalMergeFilesCount() < eagerMergeTriggerThreshold);
        } else {
            assertEquals(0, result.getEagerMergeRuns());
            assertEquals(this.numberOfIntermediateFiles, result.getFinalMergeFilesCount());
        }
        // Verify result
        List<String> actualFFS = Files.readAllLines(resultFile);
        assertArrayEquals(expectedFFS, actualFFS.toArray(new String[0]));
    }

    /**
     * For manual testing.
     */
    @Ignore
    public void manyFilesToMergeManual() throws Exception {
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, "50");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, "32");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, "512");
        System.setProperty(PipelinedMergeSortTask.OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, "8");
        Path dirWithFilesToMerge = Paths.get("/path/to/ffs/intermediate/files");
        Path[] files = Files.list(dirWithFilesToMerge)
                .filter(Files::isRegularFile)
                .toArray(Path[]::new);

        PipelinedMergeSortTask.Result result = runTestLargeFiles(new LZ4Compression(), files);
        Path resultFile = result.getFlatFileStoreFile();
        log.info("Result: {}\n{}", resultFile, Files.readString(resultFile, FLATFILESTORE_CHARSET));
    }
}
