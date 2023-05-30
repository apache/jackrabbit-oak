package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.BasicDBObject;
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
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.guava.common.collect.Iterables.concat;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.PipelinedStrategy.SENTINEL_MONGO_DOCUMENT;

public class PipelinedTransformTask implements Callable<TransformResult> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedTransformTask.class);

    private final MongoDocumentStore mongoStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final Predicate<String> pathPredicate;
    private final NodeStateEntryWriter entryWriter;
    private final int nseBatchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;
    private final ArrayBlockingQueue<ArrayList<BasicNodeStateHolder>> entryListBlocksQueue;
    private final List<File> sortedFiles = new ArrayList<>();
    private final Collection<NodeDocument> collection;

    public PipelinedTransformTask(MongoDocumentStore mongoStore,
                                  DocumentNodeStore documentNodeStore,
                                  Collection<NodeDocument> collection,
                                  RevisionVector rootRevision,
                                  Predicate<String> pathPredicate,
                                  NodeStateEntryWriter entryWriter,
                                  int nseBatchSize,
                                  ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue,
                                  ArrayBlockingQueue<ArrayList<BasicNodeStateHolder>> entryListBlocksQueue
    ) {
        this.mongoStore = mongoStore;
        this.documentNodeStore = documentNodeStore;
        this.collection = collection;
        this.rootRevision = rootRevision;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
        this.nseBatchSize = nseBatchSize;
        this.mongoDocQueue = mongoDocQueue;
        this.entryListBlocksQueue = entryListBlocksQueue;
    }

    @Override
    public TransformResult call() {
//        logFlags();
        try {
            LOG.info("Starting transform task. mongoStore={}, queue: {}", mongoStore, mongoDocQueue);

            NodeDocumentCache nodeCache = mongoStore.getNodeDocumentCache();
            Stopwatch w = Stopwatch.createStarted();
            long entryCount = 0;
            long mongoObjectsProcessed = 0;
            ArrayList<BasicNodeStateHolder> entryBatch = new ArrayList<>(nseBatchSize);
            while (true) {
                BasicDBObject[] dbObjectBatch = mongoDocQueue.take();
                if (dbObjectBatch == SENTINEL_MONGO_DOCUMENT) {
                    //Save the last batch
                    entryListBlocksQueue.put(entryBatch);
                    LOG.info("Dumped {} nodestates in json format in {}", entryCount, w);
                    LOG.info("Created {} sorted files of size {} to merge",
                            sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
                    return new TransformResult(entryCount);
                } else {
                    // Transform object
                    for (BasicDBObject dbObject : dbObjectBatch) {
                        mongoObjectsProcessed++;
                        LOG.debug("Converting: {}", dbObject);
                        if (mongoObjectsProcessed % 10000 == 0) {
                            LOG.info("Mongo objects: {}, entries: {}, entryBatch size: {}",
                                    mongoObjectsProcessed, entryCount, entryBatch.size());
                        }
                        //TODO Review the cache update approach where tracker has to track *all* docs
                        NodeDocument nodeDoc = mongoStore.convertFromDBObject(collection, dbObject);
                        nodeCache.put(nodeDoc);
                        if (!nodeDoc.isSplitDocument()) {
                            // LOG.info("Mongo path: {}", nodeDoc.get(Document.ID));
                            for (NodeStateEntry nse : getEntries(nodeDoc)) {
                                String path = nse.getPath();
                                if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
                                    String jsonText = entryWriter.asJson(nse.getNodeState());
                                    //Here logic differs from NodeStateEntrySorter in sense that
                                    //Holder line consist only of json and not 'path|json'
//                                        NodeStateHolder h = new StateInBytesHolder(path, jsonText);
                                    BasicNodeStateHolder h = new BasicNodeStateHolder(path, jsonText);
                                    entryCount++;
                                    entryBatch.add(h);
                                    if (entryBatch.size() == nseBatchSize) {
                                        entryListBlocksQueue.put(entryBatch);
                                        entryBatch = new ArrayList<>(nseBatchSize);
                                    }
                                }
                            }
                        }
                    }
                    // Eagerly release the references to objects in the array, help the GB reclaim the memory
                    Arrays.fill(dbObjectBatch, null);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Failed to traverse", e);
            throw new RuntimeException(e);
        }
    }


    private Iterable<NodeStateEntry> getEntries(NodeDocument doc) {
        Path path = doc.getPath();

        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);

        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return emptyList();
        }

        return transform(
                concat(singleton(nodeState), nodeState.getAllBundledNodesStates()),
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
