package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class PipelineMongoDownloadTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineMongoDownloadTask.class);

    private final MongoDocumentStore mongoStore;
    private final Collection<? extends Document> collection;
    private final MongoDocumentTraverser.TraversingRange traversingRange;
    private final Predicate<String> filter;
    private final int batchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;

    private final Logger traversalLog = LoggerFactory.getLogger(PipelineMongoDownloadTask.class.getName() + ".traversal");

    private static final String TRAVERSER_ID_PREFIX = "NSET";
    private static final AtomicInteger traverserInstanceCounter = new AtomicInteger(0);

    public <T extends Document> PipelineMongoDownloadTask(MongoDocumentStore mongoStore,
                                                          Collection<T> collection,
                                                          MongoDocumentTraverser.TraversingRange traversingRange,
                                                          Predicate<String> filter,
                                                          int batchSize,
                                                          ArrayBlockingQueue<BasicDBObject[]> queue) {
        this.mongoStore = mongoStore;
        this.collection = collection;
        this.traversingRange = traversingRange;
        this.batchSize = batchSize;
        // TODO
//        this.filter = filter;
        this.mongoDocQueue = queue;

        IndexingProgressReporter progressReporterPerTask =
                new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
        String entryTraverserID = TRAVERSER_ID_PREFIX + traverserInstanceCounter.incrementAndGet();
        //As first traversal is for dumping change the message prefix
        progressReporterPerTask.setMessagePrefix("Dumping from " + entryTraverserID);

        this.filter = (id) -> {
            try {
                progressReporterPerTask.traversedNode(() -> id);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
            traversalLog.trace(id);
            return true;
        };
    }

    @Override
    public void run() {
//        if (!disableReadOnlyCheck) {
//            checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
//        }
        try {
            LOG.info("Starting to download from MongoDB. mongoStore={}, collection={}, traversingRange={}",
                    mongoStore, collection, traversingRange);
            MongoCollection<BasicDBObject> dbCollection = mongoStore.getDBCollection(collection);
            //TODO This may lead to reads being routed to secondary depending on MongoURI
            //So caller must ensure that its safe to read from secondary
            Iterable<BasicDBObject> cursor;
            if (traversingRange.coversAllDocuments()) {
                cursor = dbCollection
                        .withReadPreference(mongoStore.getConfiguredReadPreference(collection))
                        .find();
            } else {
                ReadPreference preference = mongoStore.getConfiguredReadPreference(collection);
                LOG.info("Using read preference {}", preference.getName());
                cursor = dbCollection
                        .withReadPreference(preference)
                        .find(traversingRange.getFindQuery()).sort(new BsonDocument()
                                .append(NodeDocument.MODIFIED_IN_SECS, new BsonInt64(1))
                                .append(NodeDocument.ID, new BsonInt64(1)));
            }

            long count = 0;
            BasicDBObject[] block = new BasicDBObject[batchSize];
            int nextIndex = 0;
            for (BasicDBObject next : cursor) {
                String id = next.getString(Document.ID);
                count++;
                if (count % 10000 == 0) {
//                    String rate = String.format("%2.2f", count * 1000.0 / start.elapsed(TimeUnit.MILLISECONDS));
//                    LOG.info("Downloaded {} documents in {} seconds, rate: {}, next: {}",
//                            count, start.elapsed(TimeUnit.SECONDS), rate, id);
                    LOG.info("Queue size {} ", mongoDocQueue.size());
                }
                if (filter.test(id)) {
                    block[nextIndex] = next;
                    nextIndex++;
                    if (nextIndex == batchSize) {
                        mongoDocQueue.put(block);
                        block = new BasicDBObject[batchSize];
                        nextIndex = 0;
                    }
                }
            }
        } catch (RuntimeException t) {
            LOG.error("Error downloading", t);
            throw t;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
