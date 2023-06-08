package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createOutputStream;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_NSE_BUFFER;

public class PipelinedSortBatchTask implements Callable<PipelinedSortBatchTask.Result> {
    public static class Result {
        private final long totalEntries;

        public Result(long totalEntries) {
            this.totalEntries = totalEntries;
        }

        public long getTotalEntries() {
            return totalEntries;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSortBatchTask.class);

    private final Comparator<SortKey> pathComparator;
    private final Compression algorithm;
    private final ArrayBlockingQueue<NodeStateEntryBatch> emptyBuffersQueue;
    private final ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue;
    private final ArrayBlockingQueue<File> sortedFilesQueue;
    private final File sortWorkDir;
    private long entriesProcessed = 0;

    public PipelinedSortBatchTask(File storeDir,
                                  PathElementComparatorStringArray pathComparator,
                                  Compression algorithm,
                                  ArrayBlockingQueue<NodeStateEntryBatch> emptyBuffersQueue,
                                  ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue,
                                  ArrayBlockingQueue<File> sortedFilesQueue) throws IOException {
        this.pathComparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.emptyBuffersQueue = emptyBuffersQueue;
        this.nonEmptyBuffersQueue = nonEmptyBuffersQueue;
        this.sortedFilesQueue = sortedFilesQueue;
        sortWorkDir = createdSortWorkDir(storeDir);
    }

    @Override
    public Result call() {
        LOG.info("Sort entry batch task");

        try {
            while (true) {
                LOG.info("Waiting for next batch");
                NodeStateEntryBatch nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NSE_BUFFER) {
                    return new Result(entriesProcessed);
                }
                sortAndSaveBatch(nseBuffer);
                nseBuffer.reset();
                emptyBuffersQueue.put(nseBuffer);
            }
        } catch (Throwable t) {
            LOG.warn("Error", t);
            throw new RuntimeException(t);
        }
    }

    private final byte[] copyBuffer = new byte[4096];

    private void sortAndSaveBatch(NodeStateEntryBatch nseb) throws IOException, InterruptedException {
        ArrayList<SortKey> sortBuffer = nseb.getSortBuffer();
        ByteBuffer buffer = nseb.getBuffer();
        LOG.info("sortAndSaveBatch. Size of batch: {} {}", sortBuffer.size(), humanReadableByteCount(buffer.remaining()));
        if (sortBuffer.size() == 0) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        sortBuffer.sort(pathComparator);
        LOG.info("sorted in {}", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", sortWorkDir);
        long textSize = 0;
        try (BufferedOutputStream writer = createOutputStream(newtmpfile, algorithm)) {
            for (SortKey entry : sortBuffer) {
                entriesProcessed++;
                // Retrieve the entry from the buffer
                int posInBuffer = entry.getBufferPos();
                buffer.position(posInBuffer);
                int entrySize = buffer.getInt();

                // Write the entry to the file without creating intermediate byte[]
                int bytesRemaining = entrySize;
                while (bytesRemaining > 0) {
                    int bytesRead = Math.min(copyBuffer.length, bytesRemaining);
                    buffer.get(copyBuffer, 0, bytesRead);
                    writer.write(copyBuffer, 0, bytesRead);
                    bytesRemaining -= bytesRead;
                }
                writer.write('\n');

                textSize += entrySize + 1;
            }
        }
        LOG.info("Sorted and stored batch of size {} (uncompressed {}) with {} entries in {}",
                humanReadableByteCount(newtmpfile.length()), humanReadableByteCount(textSize), sortBuffer.size(), saveClock);
        sortedFilesQueue.put(newtmpfile);
    }

    private static File createdSortWorkDir(File storeDir) throws IOException {
        File sortedFileDir = new File(storeDir, "sort-work-dir");
        FileUtils.forceMkdir(sortedFileDir);
        return sortedFileDir;
    }
}
