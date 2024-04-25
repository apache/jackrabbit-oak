package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;


import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;

import java.io.Closeable;
import java.io.IOException;

class MongoTestBackend implements Closeable {
    final MongoClientURI mongoClientURI;
    final MongoDocumentStore mongoDocumentStore;
    final DocumentNodeStore documentNodeStore;
    final MongoDatabase mongoDatabase;

    public MongoTestBackend(MongoClientURI mongoClientURI, MongoDocumentStore mongoDocumentStore, DocumentNodeStore documentNodeStore, MongoDatabase mongoDatabase) {
        this.mongoClientURI = mongoClientURI;
        this.mongoDocumentStore = mongoDocumentStore;
        this.documentNodeStore = documentNodeStore;
        this.mongoDatabase = mongoDatabase;
    }

    @Override
    public void close() throws IOException {
        documentNodeStore.dispose();
        mongoDocumentStore.dispose();
    }

    @Override
    public String toString() {
        return "Backend{" +
                "mongoClientURI=" + mongoClientURI +
                ", mongoDocumentStore=" + mongoDocumentStore +
                ", documentNodeStore=" + documentNodeStore +
                ", mongoDatabase=" + mongoDatabase +
                '}';
    }
}
