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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeRunnerTest {
    private final LogCustomizer lc = LogCustomizer.forLogger(MergeRunner.class)
            .filter(Level.INFO)
            .enable(Level.INFO)
            .create();
    private final String newline = System.lineSeparator();
    private final List<File> testFiles = Lists.newArrayList();
    private final int threadPoolSize = 1,
                      batchMergeSize = 3;
    private final PathElementComparator pathComparator = new PathElementComparator();
    private final Comparator<NodeStateHolder> comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
    private final NodeStateEntryWriter entryWriter = new NodeStateEntryWriter(new MemoryBlobStore());

    @Before
    public void setup(){
        lc.starting();
    }

    @After
    public void after() {
        lc.finished();
        testFiles.clear();
    }

    @Test
    public void test() throws Exception {
        lc.starting();

        File tmpDir = new File(FileUtils.getTempDirectory(), Long.toString(System.nanoTime())),
                mergeDir = new File(tmpDir, "merge-reverse"),
                sortedFile = new File(tmpDir, "sorted-file.json");
        List<String> expectedLogOutput = Lists.newArrayList(),
                actualLogOutput = Lists.newArrayList();

        int testFileCount = 13;
        String expectedSortedFileStr = generateTestFiles(tmpDir, testFileCount);
        assertEquals("expected generated test files number does not match", testFileCount, testFiles.size());

        BlockingQueue<File> sortedFiles = new LinkedBlockingQueue<>();
        Phaser mergePhaser = new Phaser(1);
        Runnable mergeRunner = new MergeRunner(sortedFile, sortedFiles, mergeDir, comparator, mergePhaser, batchMergeSize, threadPoolSize, Compression.NONE);
        Thread merger = new Thread(mergeRunner, "test-merger");
        merger.setDaemon(true);

        sortedFiles.addAll(testFiles);
        merger.start();
        sortedFiles.add(MergeRunner.MERGE_POISON_PILL);
        mergePhaser.awaitAdvance(0);

        actualLogOutput = lc.getLogs();
        System.out.println(String.join(newline, actualLogOutput));
        String actualSortedFileStr = FileUtils.readFileToString(sortedFile, UTF_8);
        assertEquals("sorted-file content should be expected", expectedSortedFileStr, actualSortedFileStr);

        List<String> intermediateMergeLog = new ArrayList<String>(){
            {
                add("merge complete for intermediate-1");
                add("merge complete for intermediate-2");
                add("merge complete for intermediate-3");
            }
        };
        assertTrue("merge complete output should exist", actualLogOutput.containsAll(intermediateMergeLog));
        actualLogOutput.removeAll(intermediateMergeLog);

        expectedLogOutput.add("created merge task for intermediate-1 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(0));
                add(testFiles.get(1));
                add(testFiles.get(2));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-2 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(3));
                add(testFiles.get(4));
                add(testFiles.get(5));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-3 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(6));
                add(testFiles.get(7));
                add(testFiles.get(8));
            }
        }));
        expectedLogOutput.add("Waiting for batch sorting tasks completion");
        expectedLogOutput.add("There are still 4 sorted files not merged yet");
        expectedLogOutput.add("running final batch merge task for final-1 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(new File(mergeDir, "intermediate-1"));
                add(testFiles.get(9));
                add(testFiles.get(10));
            }
        }));
        expectedLogOutput.add("running final batch merge task for final-2 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(11));
                add(testFiles.get(12));
                add(new File(mergeDir, "intermediate-2"));
            }
        }));
        expectedLogOutput.add("running final batch merge task for " + sortedFile.getName() + " with " + (new ArrayList<File>(batchMergeSize){
            {
                add(new File(mergeDir, "intermediate-3"));
                add(new File(mergeDir, "final-1"));
                add(new File(mergeDir, "final-2"));
            }
        }));
        expectedLogOutput.add("Total batch sorted files length is 18");
        assertEquals("final merge log output should be expected", String.join(newline, expectedLogOutput), String.join(newline, actualLogOutput));
    }

    @Test
    public void testReverse() throws Exception {
        lc.starting();

        File tmpDir = new File(FileUtils.getTempDirectory(), Long.toString(System.nanoTime())),
             mergeDir = new File(tmpDir, "merge"),
             sortedFile = new File(tmpDir, "sorted-file.json");
        List<String> expectedLogOutput = Lists.newArrayList(),
                     actualLogOutput = Lists.newArrayList();

        int testFileCount = 16;
        String expectedSortedFileStr = generateTestFiles(tmpDir, testFileCount);
        assertEquals("expected generated test files number does not match", testFileCount, testFiles.size());

        BlockingQueue<File> sortedFiles = new LinkedBlockingQueue<>();
        Phaser mergePhaser = new Phaser(1);
        Runnable mergeRunner = new MergeRunner(sortedFile, sortedFiles, mergeDir, comparator, mergePhaser, batchMergeSize, threadPoolSize, Compression.NONE);
        Thread merger = new Thread(mergeRunner, "test-merger");
        merger.setDaemon(true);

        Collections.reverse(testFiles);
        sortedFiles.addAll(testFiles);
        merger.start();
        sortedFiles.add(MergeRunner.MERGE_POISON_PILL);
        mergePhaser.awaitAdvance(0);

        actualLogOutput = lc.getLogs();
        System.out.println(String.join(newline, actualLogOutput));
        String actualSortedFileStr = FileUtils.readFileToString(sortedFile, UTF_8);
        assertEquals("sorted-file content should be expected", expectedSortedFileStr, actualSortedFileStr);

        Collections.reverse(testFiles);
        List<String> intermediateMergeLog = new ArrayList<String>(){
            {
                add("merge complete for intermediate-1");
                add("merge complete for intermediate-2");
                add("merge complete for intermediate-3");
                add("merge complete for intermediate-4");
            }
        };
        assertTrue("merge complete output should exist", actualLogOutput.containsAll(intermediateMergeLog));
        actualLogOutput.removeAll(intermediateMergeLog);

        expectedLogOutput.add("created merge task for intermediate-1 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(10));
                add(testFiles.get(11));
                add(testFiles.get(12));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-2 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(7));
                add(testFiles.get(8));
                add(testFiles.get(9));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-3 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(4));
                add(testFiles.get(5));
                add(testFiles.get(6));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-4 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(1));
                add(testFiles.get(2));
                add(testFiles.get(3));
            }
        }));
        expectedLogOutput.add("Waiting for batch sorting tasks completion");
        expectedLogOutput.add("There are still 4 sorted files not merged yet");
        expectedLogOutput.add("running final batch merge task for final-1 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(0));
                add(new File(mergeDir, "intermediate-4"));
                add(testFiles.get(13));
            }
        }));
        expectedLogOutput.add("running final batch merge task for final-2 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(testFiles.get(14));
                add(testFiles.get(15));
                add(new File(mergeDir, "intermediate-3"));
            }
        }));
        expectedLogOutput.add("running final batch merge task for final-3 with " + (new ArrayList<File>(batchMergeSize){
            {
                add(new File(mergeDir, "final-1"));
                add(new File(mergeDir, "intermediate-2"));
                add(new File(mergeDir, "intermediate-1"));
            }
        }));
        expectedLogOutput.add("running final batch merge task for " + sortedFile.getName() + " with " + (new ArrayList<File>(batchMergeSize){
            {
                add(new File(mergeDir, "final-2"));
                add(new File(mergeDir, "final-3"));
            }
        }));
        expectedLogOutput.add("Total batch sorted files length is 23");
        assertEquals("final merge log output should be expected", String.join(newline, expectedLogOutput), String.join(newline, actualLogOutput));
    }

    // The function will generate <fileCount> files where the filename equals number of lines in the file.
    private String generateTestFiles(File tmpDir, int fileCount) throws IOException {
        LinkedList<NodeStateHolder> resultNodeState = new LinkedList<>();
        List<Integer> input = IntStream.rangeClosed(1, (1 + fileCount) * fileCount / 2).boxed().collect(Collectors.toList());
        Collections.shuffle(input);
        // cannot have duplicate because externalMerge does not take care of duplicate items
        for (int fname = 1; fname <= fileCount; fname++) {
            LinkedList<NodeStateHolder> tmpNodeState = new LinkedList<>();
            for (int line = 0; line < fname; line++) {
                NodeStateHolder nodeState = new StateInBytesHolder(String.format("/%08d", input.remove(0)), "{}");
                tmpNodeState.add(nodeState);
            }
            tmpNodeState.sort(comparator);
            resultNodeState.addAll(tmpNodeState);
            String testFileContent = "";
            while (!tmpNodeState.isEmpty()) {
                NodeStateHolder h = tmpNodeState.removeFirst();
                String text = entryWriter.toString(h.getPathElements(), h.getLine());
                testFileContent += text + newline;
            }
            File testFile = new File(tmpDir, Integer.toString(fname));
            FileUtils.writeStringToFile(testFile, testFileContent, UTF_8);
            testFiles.add(testFile);
        }

        String expectedSortedFileStr = "";
        resultNodeState.sort(comparator);
        while (!resultNodeState.isEmpty()) {
            NodeStateHolder h = resultNodeState.removeFirst();
            String text = entryWriter.toString(h.getPathElements(), h.getLine());
            expectedSortedFileStr += text + newline;
        }
        return expectedSortedFileStr;
    }
}