package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
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
import org.bson.BsonInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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

    private static final String PIPELINED_FULL_TRAVERSE = "PIPELINED_FULL_TRAVERSE";

    private final MongoDocumentStore mongoStore;
    private final Collection<? extends Document> collection;
    private final Predicate<String> filter;
    private final int batchSize;
    private final ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue;

    private final Logger traversalLog = LoggerFactory.getLogger(PipelineMongoDownloadTask.class.getName() + ".traversal");

    private static final String TRAVERSER_ID_PREFIX = "NSET";
    private static final AtomicInteger traverserInstanceCounter = new AtomicInteger(0);
    private final boolean fullTraverse;

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

        this.fullTraverse = getEnvVariableAsBoolean(PIPELINED_FULL_TRAVERSE, true);
    }

    private boolean getEnvVariableAsBoolean(String name, boolean defaultValue) {
        String value = System.getenv(name);
        boolean result;
        if (value == null) {
            result = defaultValue;
        } else {
            result = Boolean.parseBoolean(value);
        }
        LOG.info("Config {}={}", name, result);
        return result;
    }

    @Override
    public Result call() {
        Preconditions.checkState(mongoStore.isReadOnly(), "Traverser can only be used with readOnly store");
        TraversingRange traversingRange = new TraversingRange(new LastModifiedRange(0, Long.MAX_VALUE), null);
        return traverse(traversingRange);
    }

    private Result traverse(TraversingRange traversingRange) {
        // TODO: Recovery from broken connections to MongoDB
        LOG.info("Starting to download from MongoDB. traversingRange={}", traversingRange);
        MongoCollection<BasicDBObject> dbCollection = MongoDocumentStoreHelper.getDBCollection(mongoStore, collection);
        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
        Iterable<BasicDBObject> cursor;
        ReadPreference readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        LOG.info("Using read preference {}", readPreference.getName());
//            if (traversingRange.coversAllDocuments()) {
        if (fullTraverse) {
            LOG.info("Doing full traversal: {}", readPreference);
            cursor = dbCollection.withReadPreference(readPreference).find();
        } else {
            BsonDocument sortCondition = new BsonDocument()
                    .append(NodeDocument.MODIFIED_IN_SECS, new BsonInt64(1))
                    .append(NodeDocument.ID, new BsonInt64(1));
            LOG.info("Traversing range: {}", traversingRange);
            LOG.info("Find query: {}", traversingRange.getFindQuery());
            LOG.info("Sort condition: {}", sortCondition);
            cursor = dbCollection
                    .withReadPreference(readPreference)
                    .find(traversingRange.getFindQuery())
                    .sort(new BsonDocument()
                            .append(NodeDocument.MODIFIED_IN_SECS, new BsonInt64(1))
                            .append(NodeDocument.ID, new BsonInt64(1))
                    );
        }

        long documentsRead = 0;

        BasicDBObject[] block = new BasicDBObject[batchSize];
        int nextIndex = 0;

        try {
            for (BasicDBObject next : cursor) {
                String id = next.getString(Document.ID);
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
            LOG.info("Finished downloading. Elements in last block: {}", nextIndex);
            if (nextIndex > 0) {
                LOG.info("Enqueueing last block of size: {}", nextIndex);
                BasicDBObject[] lastBlock = new BasicDBObject[nextIndex];
                System.arraycopy(block, 0, lastBlock, 0, nextIndex);
                mongoDocQueue.put(lastBlock);
            }
            return new Result(documentsRead);
        } catch (MongoSocketException mongoSocketException) {
            LOG.warn("Connection error downloading from MongoDB.", mongoSocketException);
            throw new RuntimeException(mongoSocketException);
        } catch (MongoTimeoutException mongoSocketException) {
            LOG.warn("Connection error downloading from MongoDB.", mongoSocketException);
            throw new RuntimeException(mongoSocketException);
        } catch (Throwable t) {
            LOG.error("Error downloading", t);
            throw new RuntimeException(t);
        }
    }
}
