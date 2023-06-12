package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class PipelineMongoDownloadTask implements Callable<PipelineMongoDownloadTask.Result> {
    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelineMongoDownloadTask.class);

    private final MongoDocumentStore mongoStore;
    private final Collection<? extends Document> collection;
    private final Predicate<String> filter;
    private final int batchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;

    private final int maxRetries;
    private final long retryInitialIntervalMillis;
    private final long retryMaxIntervalMillis = 30_000;


    private final Logger traversalLog = LoggerFactory.getLogger(PipelineMongoDownloadTask.class.getName() + ".traversal");

    private static final String TRAVERSER_ID_PREFIX = "NSET";
    private static final AtomicInteger traverserInstanceCounter = new AtomicInteger(0);
    long documentsRead = 0;

    long nextLastModified;
    long upperBoundLastModified;
    String lastIdDownloaded = null;

    public <T extends Document> PipelineMongoDownloadTask(MongoDocumentStore mongoStore,
                                                          Collection<T> collection,
                                                          Predicate<String> filter,
                                                          int batchSize,
                                                          ArrayBlockingQueue<BasicDBObject[]> queue) {
        this.mongoStore = mongoStore;
        this.collection = collection;
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

        this.maxRetries = 3;
        this.retryInitialIntervalMillis = 1000;

    }

//    private boolean getEnvVariableAsBoolean(String name, boolean defaultValue) {
//        String value = System.getenv(name);
//        boolean result;
//        if (value == null) {
//            result = defaultValue;
//        } else {
//            result = Boolean.parseBoolean(value);
//        }
//        LOG.info("Config {}={}", name, result);
//        return result;
//    }

    @Override
    public Result call() throws Exception {
        Preconditions.checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
        LOG.info("Starting to download from MongoDB.");
        MongoCollection<BasicDBObject> dbCollection = MongoDocumentStoreHelper.getDBCollection(mongoStore, collection);
        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary

        ReadPreference readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        LOG.info("Using read preference {}", readPreference.getName());

        this.nextLastModified = getFirstLastModified(dbCollection);
        this.upperBoundLastModified = getLatestLastModified(dbCollection) + 1;
        this.lastIdDownloaded = null;
        int retries = 0;
        long retryInterval = retryInitialIntervalMillis;

        Map<String, Integer> exceptions = new HashMap<>();
        while (true) {
            try {
                if (lastIdDownloaded != null) {
                    LOG.info("Finishing partial block for _modified={}", nextLastModified);
                    // Finish the current block of documents with the current last modified value
                    downloadRange(
                            new TraversingRange(
                                    new LastModifiedRange(nextLastModified, nextLastModified + 1),
                                    lastIdDownloaded
                            ),
                            dbCollection, readPreference);
                }
                downloadRange(
                        new TraversingRange(new LastModifiedRange(nextLastModified + 1, upperBoundLastModified), null),
                        dbCollection, readPreference
                );
                return new Result(documentsRead);
            } catch (MongoException e) {
                LOG.warn("Connection error downloading from MongoDB.", e);
                if (retries == this.maxRetries) {
                    // Get a string of all exceptions that were thrown
                    StringBuilder summary = new StringBuilder();
                    for (Map.Entry<String, Integer> entry : exceptions.entrySet()) {
                        summary.append("\n\t").append(entry.getValue()).append("x: ").append(entry.getKey());
                    }
                    throw new RetryException(retries, summary.toString(), e);
                } else {
                    LOG.warn("Retrying download after {} ms; number of times failed: {}", retryInterval, retries);
                    retries++;
                    exceptions.compute(e.getClass().getSimpleName() + " - " + e.getMessage(),
                            (key, val) -> val == null ? 1 : val + 1
                    );
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ignore) {
                    }
                    // simple exponential backoff mechanism
                    retryInterval = Math.min(retryMaxIntervalMillis, retryInterval * 2);
                }
            }
        }
    }

    private void downloadRange(TraversingRange range, MongoCollection<BasicDBObject> dbCollection, ReadPreference readPreference) throws InterruptedException {
        BasicDBObject[] block = new BasicDBObject[batchSize];
        int nextIndex = 0;
        BsonDocument findQuery = range.getFindQuery();
        LOG.info("Traversing from {}: query: {}", range, findQuery);
        FindIterable<BasicDBObject> mongoIterable = dbCollection
                .withReadPreference(readPreference)
                .find(findQuery)
                .sort(ascending(NodeDocument.MODIFIED_IN_SECS, NodeDocument.ID));
        try (MongoCursor<BasicDBObject> cursor = mongoIterable.iterator()) {
            while (cursor.hasNext()) {
                BasicDBObject next = cursor.next();
                String id = next.getString(Document.ID);
                this.nextLastModified = next.getLong(NodeDocument.MODIFIED_IN_SECS);
                this.lastIdDownloaded = id;
                documentsRead++;
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
            LOG.info("Finished downloading range: {}. Elements in last block: {}", range, nextIndex);
            if (nextIndex > 0) {
                LOG.info("Enqueueing last block of size: {}", nextIndex);
                BasicDBObject[] lastBlock = new BasicDBObject[nextIndex];
                System.arraycopy(block, 0, lastBlock, 0, nextIndex);
                mongoDocQueue.put(lastBlock);
            }
        }
    }

    private long getFirstLastModified(MongoCollection<BasicDBObject> dbCollection) {
        return getModified(dbCollection, ascending(NodeDocument.MODIFIED_IN_SECS));
    }

    private long getLatestLastModified(MongoCollection<BasicDBObject> dbCollection) {
        return getModified(dbCollection, descending(NodeDocument.MODIFIED_IN_SECS));
    }

    private long getModified(MongoCollection<BasicDBObject> dbCollection, Bson sort) {
        BsonDocument query = new BsonDocument();
        query.append(NodeDocument.MODIFIED_IN_SECS, new BsonDocument().append("$ne", new BsonNull()));
        try (MongoCursor<BasicDBObject> cursor = dbCollection
                .find(query)
                .sort(sort)
                .limit(1)
                .iterator()) {
            if (!cursor.hasNext()) {
                return -1;
            }
            return cursor.next().getLong(NodeDocument.MODIFIED_IN_SECS);
        }
    }

    private static class RetryException extends RuntimeException {

        private final int tries;

        public RetryException(int tries, String message, Throwable cause) {
            super(message, cause);
            this.tries = tries;
        }

        @Override
        public String toString() {
            return "Tried " + tries + " times: \n" + super.toString();
        }
    }
}
