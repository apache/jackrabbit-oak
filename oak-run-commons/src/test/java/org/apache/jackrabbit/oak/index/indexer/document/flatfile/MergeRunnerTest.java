package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;

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
    private String expectedSortedFileStr = "",
                   actualSortedFileStr = "";

    @Before
    public void setup(){
        lc.starting();
    }

    @After
    public void after() {
        lc.finished();
    }

    @Test
    public void test() throws IOException {
        lc.starting();
        System.setProperty(FlatFileNodeStoreBuilder.PROP_MERGE_TASK_BATCH_SIZE, "3");
        System.setProperty(FlatFileNodeStoreBuilder.PROP_MERGE_THREAD_POOL_SIZE, "1");

        File tmpDir = new File(FileUtils.getTempDirectory(), Long.toString(System.nanoTime())),
             mergeDir = new File(tmpDir, "merge"),
             sortedFile = new File(tmpDir, "sorted-file.json");
        List<String> expectedLogOutput = Lists.newArrayList(),
                     actualLogOutput = Lists.newArrayList();

        generateTestFiles(tmpDir);
        assertEquals("expected 13 generated test files", 13, testFiles.size());

        PathElementComparator pathComparator = new PathElementComparator();
        Comparator<NodeStateHolder> comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        BlockingQueue<File> sortedFiles = new LinkedBlockingQueue<>();
        Phaser mergePhaser = new Phaser(1);
        Runnable mergeRunner = new MergeRunner(sortedFile, sortedFiles, mergeDir, comparator, mergePhaser, false);
        Thread merger = new Thread(mergeRunner, "test-merger");
        merger.setDaemon(true);

        // Adding test files in predefined order
        ArrayList<Integer> filenameOrder = new ArrayList<>(Arrays.asList(7, 1, 11, 9, 8, 6, 12, 4, 5, 13, 3, 2, 10));
        for (int filename: filenameOrder.subList(0,4)) {
            sortedFiles.add(testFiles.get(filename-1));
        }
        merger.start();
        for (int filename: filenameOrder.subList(4,13)) {
            sortedFiles.add(testFiles.get(filename-1));
        }
        sortedFiles.add(MergeRunner.MERGE_POISON_PILL);
        mergePhaser.awaitAdvance(0);

        actualSortedFileStr = FileUtils.readFileToString(sortedFile, UTF_8);
        String text = String.join(newline, lc.getLogs());
        System.out.println(text);

        assertEquals("sorted-file content should be expected", expectedSortedFileStr, actualSortedFileStr);

        actualLogOutput = lc.getLogs();
        expectedLogOutput.add("created merge task for intermediate-1 with " + (new ArrayList<File>(){
            {
                add(testFiles.get(0));
                add(testFiles.get(5));
                add(testFiles.get(6));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-2 with " + (new ArrayList<File>(){
            {
                add(testFiles.get(3));
                add(testFiles.get(4));
                add(testFiles.get(7));
            }
        }));
        expectedLogOutput.add("created merge task for intermediate-3 with " + (new ArrayList<File>(){
            {
                add(testFiles.get(1));
                add(testFiles.get(2));
                add(testFiles.get(8));
            }
        }));
        expectedLogOutput.add("Waiting for batch sorting tasks completion");
        assertEquals("intermediate merge log output should be expected", String.join(newline, expectedLogOutput), String.join(newline, actualLogOutput.subList(0,4)));

        assertTrue("merge complete output should exist", actualLogOutput.containsAll((new ArrayList<String>(){
            {
                add("merge complete for intermediate-1");
                add("merge complete for intermediate-2");
                add("merge complete for intermediate-3");
            }
        })));

        expectedLogOutput.clear();
        expectedLogOutput.add("There are still 4 sorted files not merged yet");
        expectedLogOutput.add("running final batch merge task for final-1 with " + (new ArrayList<File>(){
            {
                add(testFiles.get(9));
                add(testFiles.get(10));
                add(testFiles.get(11));
            }
        }));
        expectedLogOutput.add("running final batch merge task for final-2 with " + (new ArrayList<File>(){
            {
                add(testFiles.get(12));
                add(new File(mergeDir, "intermediate-1"));
                add(new File(mergeDir, "intermediate-3"));
            }
        }));
        expectedLogOutput.add("running final batch merge task for " + sortedFile.getName() + " with " + (new ArrayList<File>(){
            {
                add(new File(mergeDir, "intermediate-2"));
                add(new File(mergeDir, "final-1"));
                add(new File(mergeDir, "final-2"));
            }
        }));
        expectedLogOutput.add("Total batch sorted files length is 18");
        assertEquals("final merge log output should be expected", String.join(newline, expectedLogOutput), String.join(newline, actualLogOutput.subList(7, 12)));
    }

    private void generateTestFiles(File tmpDir) throws IOException {
        int nextFileSize = 1;
        File testFile = new File(tmpDir, Integer.toString(nextFileSize));
        List<String> lineBuffer = Lists.newArrayList();
        List<String> resultList = Lists.newArrayList();
        for (int i = 0; i <= 90; i++) {
            String line = String.format("/%05d|{}", i);
            lineBuffer.add(line);
            resultList.add(line);
            if (lineBuffer.size() == nextFileSize) {
                String text = String.join(newline, lineBuffer);
                FileUtils.writeStringToFile(testFile, text, UTF_8);
                testFiles.add(testFile);
                nextFileSize += 1;
                testFile = new File(tmpDir, Integer.toString(nextFileSize));
                lineBuffer.clear();
            }
        }
        expectedSortedFileStr = String.join(newline, resultList) + newline;
    }

//    private static List<File> getResourceFolderFiles (String folder) {
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//        URL url = loader.getResource(folder);
//        String path = url.getPath();
//        return Arrays.asList(new File(path).listFiles());
//    }


//    String log = runIndexingTest(IndexFieldProviderImpl.class, true);
//    assertEquals("[" + "Added augmented fields: jcr:content/metadata/predictedTags/[my, a, my:a], 10.0" + "]", log);
}