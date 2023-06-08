package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.BasicDBObject;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import static org.apache.jackrabbit.guava.common.collect.Iterables.concat;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_MONGO_DOCUMENT;

public class PipelinedTransformTask implements Callable<PipelinedTransformTask.Result> {

    public static class Result {
        private final long entryCount;

        public Result(long entryCount) {
            this.entryCount = entryCount;
        }

        public long getEntryCount() {
            return entryCount;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedTransformTask.class);

    private final MongoDocumentStore mongoStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final Predicate<String> pathPredicate;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue;
    private final ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue;
    private final Collection<NodeDocument> collection;
    private final NodeStateEntryWriter entryWriter;

    public PipelinedTransformTask(MongoDocumentStore mongoStore,
                                  DocumentNodeStore documentNodeStore,
                                  Collection<NodeDocument> collection,
                                  RevisionVector rootRevision,
                                  Predicate<String> pathPredicate,
                                  NodeStateEntryWriter entryWriter,
                                  ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                                  ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue,
                                  ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue
    ) {
        this.mongoStore = mongoStore;
        this.documentNodeStore = documentNodeStore;
        this.collection = collection;
        this.rootRevision = rootRevision;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
        this.mongoDocQueue = mongoDocQueue;
        this.emptyBatchesQueue = emptyBatchesQueue;
        this.nonEmptyBatchesQueue = nonEmptyBatchesQueue;
    }

    @Override
    public Result call() {
        try {
            LOG.info("Starting transform task. mongoStore={}, queue: {}", mongoStore, mongoDocQueue);

            NodeDocumentCache nodeCache = MongoDocumentStoreHelper.getNodeDocumentCache(mongoStore);
            Stopwatch w = Stopwatch.createStarted();
            long totalEntryCount = 0;
            long mongoObjectsProcessed = 0;
            LOG.info("Waiting for an empty buffer");
            NodeStateEntryBatch nseBatch = emptyBatchesQueue.take();
            ArrayList<SortKey> sortArray = nseBatch.getSortBuffer();
            ByteBuffer nseBuffer = nseBatch.getBuffer();

            PathSortKeyFactory pathSortKeyFactory = new PathSortKeyFactory();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
            OutputStreamWriter writer = new OutputStreamWriter(baos, StandardCharsets.UTF_8);
            LOG.info("Obtained an empty buffer. Starting to convert Mongo documents to node state entries");
            while (true) {
                BasicDBObject[] dbObjectBatch = mongoDocQueue.take();
                if (dbObjectBatch == SENTINEL_MONGO_DOCUMENT) {
                    //Save the last batch
                    nseBatch.getBuffer().flip();
                    nonEmptyBatchesQueue.put(nseBatch);
                    LOG.info("Dumped {} nodestates in json format in {}", totalEntryCount, w);
                    return new Result(totalEntryCount);
                } else {
                    // Transform object
                    for (BasicDBObject dbObject : dbObjectBatch) {
                        mongoObjectsProcessed++;
                        LOG.debug("Converting: {}", dbObject);
                        if (mongoObjectsProcessed % 10000 == 0) {
                            LOG.info("Mongo objects: {}, total entries: {}, current batch: {}, Size: {}/{} MB",
                                    mongoObjectsProcessed, totalEntryCount, sortArray.size(),
                                    nseBuffer.position() / FileUtils.ONE_MB,
                                    nseBuffer.capacity() / FileUtils.ONE_MB
                            );
                        }
                        //TODO Review the cache update approach where tracker has to track *all* docs
                        NodeDocument nodeDoc = MongoDocumentStoreHelper.convertFromDBObject(mongoStore, collection, dbObject);
                        nodeCache.put(nodeDoc);
                        if (!nodeDoc.isSplitDocument()) {
                            // LOG.info("Mongo path: {}", nodeDoc.get(Document.ID));
                            for (NodeStateEntry nse : getEntries(nodeDoc)) {
                                String path = nse.getPath();
                                if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
                                    //Here logic differs from NodeStateEntrySorter in sense that
                                    //Holder line consist only of json and not 'path|json'
                                    entryWriter.writeTo(writer, nse);
                                    writer.flush();
                                    byte[] entryData = baos.toByteArray();
                                    baos.reset();
                                    if (nseBatch.isAtMaxEntries() || entryData.length + 4 > nseBuffer.remaining()) {
                                        LOG.info("Buffer full, passing buffer to sort task. Total entries: {}, entries in buffer {}, buffer size: {}",
                                                totalEntryCount, sortArray.size(), nseBuffer.position());
                                        nseBuffer.flip();
                                        Stopwatch putStart = Stopwatch.createStarted();
                                        nonEmptyBatchesQueue.put(nseBatch);
                                        LOG.info("Added buffer to queue in {}", putStart);
                                        nseBatch = emptyBatchesQueue.take();
                                        sortArray = nseBatch.getSortBuffer();
                                        nseBuffer = nseBatch.getBuffer();
                                        LOG.info("Sort buffer after exchange: {}", sortArray.size());
                                    }
                                    // Write entry to buffer
                                    int bufferPos = nseBuffer.position();
                                    nseBuffer.putInt(entryData.length);
                                    nseBuffer.put(entryData);
                                    String[] key = pathSortKeyFactory.genSortKey(nse.getPath());
                                    sortArray.add(new SortKey(key, bufferPos));
                                    totalEntryCount++;
                                }
                            }
                        }
                    }
                    // Eagerly release the references to objects in the array, help the GB reclaim the memory
                    Arrays.fill(dbObjectBatch, null);
                }
            }
        } catch (Throwable t) {
            LOG.error("Failed to traverse", t);
            throw new RuntimeException(t);
        }
    }

//    private boolean writeEntry(ByteBuffer buffer, String path, String entryAsJson) {
//        int sizePrefixPosition = buffer.position();
//        // Leave space to write the size of the entry.
//        int byteDataPosition = sizePrefixPosition + 4;
//        if (buffer.remaining() <= 4) {
//            // There is no space to write the entry. This check ensures that the call to set the position below will not fail
//            return false;
//        }
//        buffer.mark();
//        try {
//            // First write the entry to the buffer, we can compute the size afterwards.
//            buffer.position(byteDataPosition);
//            buffer.put(path.getBytes(StandardCharsets.UTF_8));
//            buffer.put(DELIMITER_BYTE_ARRAY);
//            buffer.put(entryAsJson.getBytes(StandardCharsets.UTF_8));
//            // The write above advanced the position to after the byte array representation of the entry,
//            // so now we can compute its size
//            int entrySize = buffer.position() - byteDataPosition;
//            // And prefix it in the buffer
//            buffer.putInt(sizePrefixPosition, entrySize);
//            // The absolute write above does not change the position in the buffer that was left after writing the entry
//            return true;
//        } catch (BufferOverflowException ex) {
//            // Not enough space to write the entry. Reset the buffer
//            LOG.info("Not enough space to write entry: {} to buffer: {}. Space remaining: {}", path, buffer, buffer.remaining());
//            buffer.reset();
//            LOG.info("Buffer after reset: {}", buffer);
//            return false;
//        }
//    }

    private Iterable<NodeStateEntry> getEntries(NodeDocument doc) {
        Path path = doc.getPath();

        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);

        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return List.of();
        }

        return transform(
                concat(List.of(nodeState), nodeState.getAllBundledNodesStates()),
                dns -> {
                    NodeStateEntry.NodeStateEntryBuilder builder = new NodeStateEntry.NodeStateEntryBuilder(dns, dns.getPath().toString());
                    if (doc.getModified() != null) {
                        builder.withLastModified(doc.getModified());
                    }
                    builder.withID(doc.getId());
                    return builder.build();
                }
        );
    }
}
