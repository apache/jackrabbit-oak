package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;

import com.mongodb.BasicDBObject;

public class MongoClearCollections {

    /**
     * Removes all documents from nodes collection.
     * 
     * @param mongoConnection
     */
    public static void clearNodesCollection(MongoConnection mongoConnection) {
        mongoConnection.getNodeCollection().remove(new BasicDBObject());
    }

    /**
     * Removes all documents from commits collection.
     * 
     * @param mongoConnection
     */
    public static void clearCommitsCollection(MongoConnection mongoConnection) {
        mongoConnection.getCommitCollection().remove(new BasicDBObject());
    }

    /**
     * Removes all documents from head collection.
     * 
     * @param mongoConnection
     */
    public static void clearHeadCollection(MongoConnection mongoConnection) {
        mongoConnection.getHeadCollection().remove(new BasicDBObject());
    }

    /**
     * Removes all documents from all microkernel collections.
     * 
     * @param mongoConnection
     */
    public static void clearAllCollections(MongoConnection mongoConnection) {
        clearNodesCollection(mongoConnection);
        clearCommitsCollection(mongoConnection);
        clearHeadCollection(mongoConnection);
    }
}
