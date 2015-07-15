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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.io.LazyInputStream;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TextExtractor implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);
    private static final Logger parserError = LoggerFactory.getLogger("org.apache.jackrabbit.oak.plugins.tika.ParserError");
    private static final int PROGRESS_BATCH_SIZE = 1000;
    private static final int MAX_EXTRACT_LENGTH = 100000;
    private static final String ERROR_TEXT = "TextExtractionError";

    private final TextWriter textWriter;

    private final WorkItem SHUTDOWN_SIGNAL = new WorkItem(null);
    private BlockingQueue<WorkItem> inputQueue;
    private ExecutorService executorService;
    private int threadPoolSize = Runtime.getRuntime().availableProcessors();
    private int queueSize = 100;

    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicLong timeTaken = new AtomicLong();
    private final AtomicInteger extractionCount = new AtomicInteger();
    private final AtomicInteger textWrittenCount = new AtomicInteger();
    private final AtomicInteger parserErrorCount = new AtomicInteger();
    private final AtomicInteger processedCount = new AtomicInteger();
    private final AtomicInteger emptyCount = new AtomicInteger();
    private final AtomicInteger notSupportedCount = new AtomicInteger();
    private final AtomicInteger alreadyExtractedCount = new AtomicInteger();
    private final AtomicLong extractedTextSize = new AtomicLong();
    private final AtomicLong nonEmptyExtractedTextSize = new AtomicLong();
    private final AtomicLong totalSizeRead = new AtomicLong();

    private int maxExtractedLength = MAX_EXTRACT_LENGTH;
    private File tikaConfig;
    private TikaHelper tika;
    private boolean initialized;
    private BinaryStats stats;
    private boolean closed;

    public TextExtractor(TextWriter textWriter) {
        this.textWriter = textWriter;
    }

    public void extract(Iterable<BinaryResource> binaries) throws InterruptedException, IOException {
        initialize();
        for (BinaryResource binary : binaries) {
            inputQueue.put(new WorkItem(binary));
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (!inputQueue.isEmpty()) {
            log.info("Shutting down the extractor. Pending task count {}", inputQueue.size());
        }

        if (executorService != null) {
            try {
                inputQueue.put(SHUTDOWN_SIGNAL);
                executorService.shutdown();
                //Wait long enough
                executorService.awaitTermination(10, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        dumpStats();
        closed = true;
    }

    public void setTikaConfig(File tikaConfig) {
        this.tikaConfig = tikaConfig;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public void setStats(BinaryStats stats) {
        this.stats = stats;
    }

    private void dumpStats() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println("Text extraction stats");
        pw.printf("\t Processed Count           : %d%n", processedCount.get());
        pw.printf("\t   Extraction Count        : %d%n", extractionCount.get());
        pw.printf("\t     Empty Count           : %d%n", emptyCount.get());
        pw.printf("\t     Text Written Count    : %d%n", textWrittenCount.get());
        pw.printf("\t   Parser Error Count      : %d%n", parserErrorCount.get());
        pw.printf("\t   Error Count             : %d%n", errorCount.get());
        pw.printf("\t   Not Supported Count     : %d%n", notSupportedCount.get());
        pw.printf("\t   Already processed Count : %d%n", alreadyExtractedCount.get());
        pw.printf("\t Total bytes read          : %s%n", IOUtils.humanReadableByteCount(totalSizeRead.get()));
        pw.printf("\t Total text extracted      : %s%n", IOUtils.humanReadableByteCount(extractedTextSize.get()));
        pw.printf("\t   Non empty text          : %s%n", IOUtils.humanReadableByteCount(nonEmptyExtractedTextSize.get()));
        pw.printf("\t Time taken                : %d sec%n", timeTaken.get() / 1000);
        pw.close();
        log.info(sw.toString());
    }

    private void dumpProgress(int count) {
        if (count % PROGRESS_BATCH_SIZE == 0) {
            String progress = "";
            if (stats != null) {
                double processedPercent = count * 1.0 / stats.getTotalCount() * 100;
                double indexedPercent = extractionCount.get() * 1.0 / stats.getIndexedCount() * 100;
                progress = String.format("(%1.2f%%) (Extraction stats %d/%d %1.2f%%, Ignored count %d)",
                        processedPercent, extractionCount.get(), stats.getIndexedCount(),
                        indexedPercent, notSupportedCount.get());
            }
            log.info("Processed {} {} binaries so far ...", count, progress);
        }
    }

    private synchronized void initialize() throws IOException {
        if (initialized) {
            return;
        }
        inputQueue = new ArrayBlockingQueue<WorkItem>(queueSize);
        tika = new TikaHelper(tikaConfig);
        initializeExecutorService();
        initialized = true;
    }

    private void extractText(BinaryResource source) throws IOException {
        String type = source.getMimeType();
        if (type == null || !tika.isSupportedMediaType(type)) {
            log.trace("Ignoring binary content for node {} due to unsupported " +
                    "(or null) jcr:mimeType [{}]", source, type);
            notSupportedCount.incrementAndGet();
            return;
        }

        String blobId = source.getBlobId();
        if (textWriter.isProcessed(blobId)) {
            alreadyExtractedCount.incrementAndGet();
            return;
        }

        //TODO Handle case where same blob is being concurrently processed
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, type);
        if (source.getEncoding() != null) { // not mandatory
            metadata.set(Metadata.CONTENT_ENCODING, source.getEncoding());
        }

        String extractedContent = parseStringValue(source.getByteSource(), metadata, source.getPath());
        if (ERROR_TEXT.equals(extractedContent)) {
            textWriter.markError(blobId);
        } else if (extractedContent != null) {
            extractedContent = extractedContent.trim();
            if (!extractedContent.isEmpty()) {
                nonEmptyExtractedTextSize.addAndGet(extractedContent.length());
                textWriter.write(blobId, extractedContent);
                textWrittenCount.incrementAndGet();
            } else {
                textWriter.markEmpty(blobId);
                emptyCount.incrementAndGet();
            }
        }
    }

    private void initializeExecutorService() {
        executorService = Executors.newFixedThreadPool(threadPoolSize);
        for (int i = 0; i < threadPoolSize; i++) {
            executorService.submit(new Extractor());
        }
        log.info("Initialized text extractor pool with {} threads", threadPoolSize);
    }

    private class Extractor implements Runnable {
        @Override
        public void run() {
            while (true) {
                WorkItem workItem = null;
                try {
                    workItem = inputQueue.take();
                    if (workItem == SHUTDOWN_SIGNAL) {
                        inputQueue.put(SHUTDOWN_SIGNAL); //put back for other workers
                        return;
                    }
                    extractText(workItem.source);
                    dumpProgress(processedCount.incrementAndGet());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    log.warn("Error occurred while processing {}", workItem, e);
                }
            }
        }
    }

    //~--------------------------------------< Tika >

    private String parseStringValue(ByteSource byteSource, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler(maxExtractedLength);
        long start = System.currentTimeMillis();
        long size = 0;
        try {
            CountingInputStream stream = new CountingInputStream(new LazyInputStream(byteSource));
            try {
                tika.getParser().parse(stream, handler, metadata, new ParseContext());
            } finally {
                size = stream.getCount();
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture and ignore errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!handler.isWriteLimitReached(t)) {
                parserErrorCount.incrementAndGet();
                parserError.debug("Failed to extract text from a binary property: "
                        + path
                        + " This is a fairly common case, and nothing to"
                        + " worry about. The stack trace is included to"
                        + " help improve the text extraction feature.", t);
                return ERROR_TEXT;
            }
        }
        String result = handler.toString();
        timeTaken.addAndGet(System.currentTimeMillis() - start);
        if (size > 0) {
            extractedTextSize.addAndGet(result.length());
            extractionCount.incrementAndGet();
            totalSizeRead.addAndGet(size);
            return result;
        }

        return null;
    }

    //~--------------------------------------< WorkItem >

    private static class WorkItem {
        final BinaryResource source;

        private WorkItem(BinaryResource source) {
            this.source = source;
        }

        @Override
        public String toString() {
            return source != null ? source.toString() : "<EMPTY>";
        }
    }

}
