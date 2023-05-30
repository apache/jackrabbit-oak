package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.PipelinedStrategy.SENTINEL_ENTRY_BATCH;

public class PipelinedSortBatchTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSortBatchTask.class);

    private final NodeStateEntryWriter entryWriter;
    private final Compression algorithm;
    private final int sortBufferSize;
    private final ArrayBlockingQueue<ArrayList<BasicNodeStateHolder>> entryBatchQueue;
    private final Comparator<NodeStateHolder> comparator;
    private final ArrayBlockingQueue<File> sortedFilesQueue;
    private final File sortWorkDir;

    private final ArrayList<BasicNodeStateHolder> sortBuffer;

    public PipelinedSortBatchTask(Comparator<NodeStateHolder> comparator,
                                  File storeDir,
                                  NodeStateEntryWriter entryWriter,
                                  Compression algorithm,
                                  int sortBufferSize,
                                  ArrayBlockingQueue<ArrayList<BasicNodeStateHolder>> entryBatchQueue,
                                  ArrayBlockingQueue<File> sortedFilesQueue) throws IOException {
        this.entryWriter = entryWriter;
        this.algorithm = algorithm;
        this.sortBufferSize = sortBufferSize;
        this.sortBuffer = new ArrayList<>(sortBufferSize);
        this.entryBatchQueue = entryBatchQueue;
        this.comparator = comparator;
        this.sortedFilesQueue = sortedFilesQueue;
        sortWorkDir = createdSortWorkDir(storeDir);
    }

    @Override
    public void run() {
        LOG.info("Sort entry batch task");

        long i = 0;
        while (true) {
            try {
                ArrayList<BasicNodeStateHolder> nseBatch = entryBatchQueue.take();
                if (nseBatch == SENTINEL_ENTRY_BATCH) {
                    sortAndSaveBatch();
                    return;
                }
                for (BasicNodeStateHolder e : nseBatch) {
                    sortBuffer.add(e);
                    if (sortBuffer.size() == sortBufferSize) {
                        sortAndSaveBatch();
                    }
                    if (i % 10000 == 0) {
                        LOG.info("Entries: {}, sort buffer size: {}", i, sortBuffer.size());
                    }
                    i++;
                }
                nseBatch.clear();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void sortAndSaveBatch() throws IOException {
        LOG.info("sortAndSaveBatch. Size of batch: {}", sortBuffer.size());
        if (sortBuffer.isEmpty()) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        sortBuffer.sort(comparator);
        LOG.info("sorted in {}", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", sortWorkDir);
        long textSize = 0;
        try (BufferedWriter writer = createWriter(newtmpfile, algorithm)) {
            for (BasicNodeStateHolder h : sortBuffer) {
                //Here holder line only contains nodeState json
                entryWriter.writeTo(writer, h.getPathElements(), h.getLine());
                writer.newLine();
            }
        }
        LOG.info("Sorted and stored batch of size {} (uncompressed {}) with {} entries in {}",
                humanReadableByteCount(newtmpfile.length()), humanReadableByteCount(textSize), sortBuffer.size(), saveClock);
        sortBuffer.clear();
        sortedFilesQueue.offer(newtmpfile);
    }

    private static File createdSortWorkDir(File storeDir) throws IOException {
        File sortedFileDir = new File(storeDir, "sort-work-dir");
        FileUtils.forceMkdir(sortedFileDir);
        return sortedFileDir;
    }
}
